# QuiverDB API Guide (v1.0.0)

This document describes the developer-facing API for QuiverDB: Rust interfaces (Db/Pager/Directory), crash-safety contract, locking semantics, environment variables, and CLI mapping.

Audience: Rust developers embedding QuiverDB or building tooling around it.

Status: v1.0.0 (formats frozen: meta v3, page v2, WAL v1)


## 1) Concepts

- Single-writer, multi-reader:
    - Writer opens with an exclusive lock and may modify files.
    - Readers open with a shared lock and must not change files.
- Crash safety via WAL:
    - All changes are appended to WAL (CRC-protected, monotonic LSN).
    - Replay applies page images with LSN-gating (apply only if `wal_lsn > page_lsn`).
    - On clean shutdown: WAL is truncated to header (fast start).
- Pages:
    - v2 64-byte header with LSN and CRC, types: KV_RH (Robin Hood) and OVERFLOW.
    - KV_RH stores key/value pairs in-page with a Robin Hood index.
    - OVERFLOW stores large values in chains referenced by placeholders from KV pages.
- Directory:
    - Per-bucket heads — forms chains of KV pages (head → next → …).
    - Atomic updates (tmp+rename) + header CRC.
- Free-list:
    - Tracks freed page IDs for reuse.


## 2) Locking and modes

- Writer (read-write):
    - `Db::open(path) -> Db`
    - Acquires an exclusive file lock.
    - Executes `wal_replay_if_any` if needed.
    - Sets `clean_shutdown=false` on open, restores true on Drop.
- Reader (read-only):
    - `Db::open_ro(path) -> Db`
    - Acquires a shared file lock.
    - Does NOT replay WAL and does NOT change `clean_shutdown`.
    - Drop does not run space maintenance.

The lock file is `<root>/LOCK` (fs2 advisory locks). On Windows, semantics are best-effort but tested by integration tests.


## 3) Crash-safety and commit contract

Commit sequence for a page write:
1. Compute new LSN = `meta.last_lsn + 1`.
2. If page is v2, embed `lsn` into header and update page CRC.
3. Append WAL record (header + payload), fsync WAL (group-commit).
4. Write data page to its segment file (fsync configurable by `P1_DATA_FSYNC`).
5. Optionally rotate (truncate) WAL — only when data fsync is enabled.
6. Update `meta.last_lsn`.
7. Update page cache entry (if enabled).

Replay (writer only):
- If `meta.clean_shutdown == false`, reads WAL frames safely (CRC + length + partial tails).
- Ignores unknown record types (forward-compatible).
- Applies page images with LSN-gating to v2 pages.
- Truncates WAL to header after success; updates `last_lsn` best-effort.


## 4) Rust API: Db (high-level)

Writer:
use anyhow::Result;
use QuiverDB::{Db, Directory, init_db};
``` rust
    fn main() -> Result<()> {
        let root = std::path::Path::new("./db");
        if !root.exists() {
            init_db(root, 4096)?;
            Directory::create(root, 128)?;
        }

        // Writer (exclusive)
        {
            let mut db = Db::open(root)?;
            db.put(b"alpha", b"1")?;
            let v = db.get(b"alpha")?.unwrap();
            assert_eq!(v, b"1");
            let deleted = db.del(b"alpha")?;
            assert!(deleted);
        }

        // Reader (shared)
        {
            let db_ro = Db::open_ro(root)?;
            let found = db_ro.get(b"alpha")?;
            assert!(found.is_none());
        }
        Ok(())
    }
```
- `Db::open`: exclusive lock, WAL replay if needed, sets dirty on open and clean on drop.
- `Db::open_ro`: shared lock, no replay and no meta mutations, drop does nothing extra.
- `put(key, val)`: inserts or updates. Large values may spill to overflow chains.
- `get(key)`: resolves overflow placeholders transparently and returns full value.
- `del(key)`: deletes a key and frees its overflow chain if present.
- `print_stats()`: prints database stats and metrics summary.

Notes:
- In page chains, the newest copy is always the last encountered (tail). Db::get chooses the last match to avoid stale reads if an older page LSN increments due to other keys.
- `Db::put` removes older duplicates of a key during/after successful update and splices-out empty pages to keep chains compact.


## 5) Rust API: Pager (low-level)

For low-level tooling/testing (bypasses the KV layer). Use with care:

- `Pager::open(root) -> Pager`
- `allocate_pages(n) -> start_id`
- `allocate_one_page() -> page_id` (prefer reusing from free-list)
- `ensure_allocated(page_id)` — makes sure segment exists and has enough length
- `write_page_raw(page_id, &buf)` — writes page without WAL (used by replay)
- `commit_page(page_id, &mut buf)` — writes via WAL + fsync (uses sequence above)
- `read_page(page_id, &mut buf)`

Example:
use anyhow::Result;
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{rh_page_init, rh_page_update_crc};
``` rust
    fn commit_example(root: &std::path::Path) -> Result<()> {
        let mut pager = Pager::open(root)?;
        let pid = pager.allocate_one_page()?;
        let ps = pager.meta.page_size as usize;

        let mut page = vec![0u8; ps];
        rh_page_init(&mut page, pid)?;           // format as KV_RH
        rh_page_update_crc(&mut page)?;
        pager.commit_page(pid, &mut page)?;      // WAL + data write
        Ok(())
    }
```
Page cache:
- Enable with `P1_PAGE_CACHE_PAGES=N`.
- Hit/miss counters are tracked in metrics.


## 6) Directory and free-list

- `Directory::create(root, buckets)` — creates the directory file with heads=NO_PAGE and header CRC.
- `Directory::open(root)` — validates magic/version and CRC.
- `head(bucket) -> page_id` and internal updates (`set_head`) are atomic (tmp+rename).

Free-list:
- Managed via file `<root>/free` with magic/version and appended `u64` page IDs.
- `FreeList::open`, `push(page_id)`, `pop() -> Option<u64>`, `count()`.


## 7) WAL (advanced)

Most users should not use WAL directly (Db/Pager handle it). Advanced APIs:
- `wal::Wal::open_for_append(root)`
- `wal::Wal::append_page_image(lsn, page_id, payload)`
- `wal::Wal::fsync()`, `wal::Wal::maybe_truncate()`
- `wal_replay_if_any(root)` — writer-safe startup recovery

CDC tools (CLI):
- `wal-tail` prints JSONL frames (type/lsn/page_id/len/crc32).
- `wal-ship` emits the raw binary stream (header + records) and handles mid-stream header/truncate.
- `wal-apply` reads from stdin and applies frames idempotently (LSN-gated for v2 pages).


## 8) Environment variables

- `P1_WAL_COALESCE_MS=` integer (default 3) — WAL fsync coalescing delay (ms).
- `P1_DATA_FSYNC=0|1` (default 1) — fsync data segments per commit:
- When `0`: rely solely on WAL durability; WAL is not truncated at commit and will truncate during replay.
- `P1_PAGE_CACHE_PAGES=N` — size of the read cache (0 disables).
- `P1_OVF_THRESHOLD_BYTES=N` — value size threshold for overflow (default: page_size/4).
- `P1_RH_COMPACT_DEAD_FRAC`, `P1_RH_COMPACT_MIN_BYTES` — RH auto-compaction heuristics.
- `P1_DB_GET_OUT=path` — raw output file for `dbget` values.


## 9) Error handling

- Common `anyhow::Result` returns; errors include I/O errors and logical validation failures (CRC mismatch, invalid magic).
- Replay will skip partial frames or CRC mismatches at the tail (treats as normal EOF).
- `check --strict` exits with error on directory failure, CRC/IO issues, or overflow orphans.


## 10) CLI ↔ API mapping

- `dbinit` (Directory::create), `dbput/dbget/dbdel/dbstats` (Db)
- `pagefmt-rh/rh-put/rh-get/rh-list/rh-del/rh-compact` (page_rh)
- `check/repair` (integrity tooling on top of Pager/Directory/overflow)
- `wal-*` (CDC tooling around WAL)
- `free-*` (FreeList operations)


## 11) Platform notes

- Windows: fsync of directory entries after `rename` is best-effort.
- Endianness: all integers on disk are little-endian.