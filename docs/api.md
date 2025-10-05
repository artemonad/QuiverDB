# QuiverDB API Guide (v1.2, Phase 1)

Audience: Rust developers embedding QuiverDB or building tooling around it.  
Status: v1.2 (Phase 1 snapshots/backup); on-disk formats remain frozen: meta v3, page v2, WAL v1.

Links:
- CDC guide: docs/cdc.md
- On-disk format: docs/format.md
- FFI (C ABI): docs/ffi.md
- Snapshots/Backup details: docs/snapshots.md

Table of contents
- 1) Concepts
- 2) Locking and modes
- 3) Configuration (QuiverConfig/DbBuilder)
- 4) Crash-safety and commit contract
- 5) Rust API: Db (high-level)
- 6) Rust API: Pager (low-level)
- 7) Directory and free-list
- 8) WAL (advanced)
- 9) Environment variables
- 10) Error handling
- 11) CLI ↔ API mapping
- 12) Platform notes
- 13) On-disk formats (summary)
- 14) Practical examples

---

## 1) Concepts

- Single-writer, multi-reader:
  - Writer opens with an exclusive lock and may modify files.
  - Readers open with a shared lock and never mutate files or meta.
- Crash safety via WAL:
  - All writes append page images to WAL (CRC-protected, monotonic LSN).
  - Replay is tolerant to partial tails and unknown record types.
  - v2 pages (KV_RH/OVERFLOW) are applied only when wal_lsn > page_lsn (LSN-gating).
- Pages:
  - v2 pages have a 64-byte header: MAGIC, version=2, type (KV_RH=2 | OVERFLOW=3), page_id, next_page_id, lsn, crc32.
  - KV_RH stores key/value pairs with a Robin Hood index.
  - OVERFLOW stores large values in chains referenced from KV as a fixed 18-byte placeholder.
- Directory (dir file):
  - Array of bucket heads; KV pages form chains (head → next → …).
  - Updates are atomic (tmp+rename) and protect with header CRC.
- Free-list:
  - Tracks freed page IDs for reuse.
- Tail-wins semantics within a chain:
  - If multiple versions of a key exist along a bucket chain, the last one (closer to the tail) is authoritative.

New in v1.2 (Phase 1)
- Snapshot isolation for reads by LSN:
  - In-process snapshots (not persisted across restarts).
  - Page-level copy-on-write (COW) via “freeze” before overwrite/free.
  - Snapshot reads choose live or frozen page “as of” snapshot_lsn (see docs/snapshots.md).
- Backup/Restore on top of snapshots:
  - Full and incremental backup (since_lsn).
  - Restore with CRC checks, sets clean_shutdown=true.
  - No dedup in Phase 1 (Phase 2 adds content-hash dedup).

---

## 2) Locking and modes

- Writer (read-write):
  - Db::open(root) or Db::open_with_config(root, cfg)
  - Exclusive file lock (fs2).
  - Executes WAL replay if needed.
  - Sets clean_shutdown=false on open and clean_shutdown=true on Drop.
  - Performs space maintenance on Drop (best-effort).

- Reader (read-only):
  - Db::open_ro(root) or Db::open_ro_with_config(root, cfg)
  - Shared file lock (fs2).
  - Does NOT replay WAL and does NOT change clean_shutdown.
  - Drop does not run space maintenance.

Lock file path: <root>/LOCK. On Windows, advisory locks are best-effort and guarded by tests.

---

## 3) Configuration (QuiverConfig/DbBuilder)

QuiverDB uses a central config object with a builder. Backward compatibility with environment variables is preserved.

Fields:
- wal_coalesce_ms: u64 — group-commit fsync coalescing window (ms). Default: 3.
- data_fsync: bool — fsync data segments on commit. Default: true.
- page_cache_pages: usize — page cache size (0 disables). Default: 0.
- ovf_threshold_bytes: Option<usize> — overflow threshold:
  - None → page_size/4.

Sources:
- By default, Db::open/open_ro read env vars into QuiverConfig (old behavior).
- Explicit config via Db::open_with_config/open_ro_with_config overrides env.

Example:

```rust
use anyhow::Result;
use QuiverDB::{Db, Directory, init_db};
use QuiverDB::config::DbBuilder;

fn main() -> Result<()> {
    let root = std::path::Path::new("./db");
    if !root.exists() {
        init_db(root, 4096)?;
        Directory::create(root, 128)?;
    }

    let cfg = DbBuilder::new()
        .wal_coalesce_ms(0)
        .data_fsync(true)
        .page_cache_pages(256)
        .ovf_threshold_bytes(None)
        .build();

    // Writer
    {
        let mut db = Db::open_with_config(root, cfg.clone())?;
        db.put(b"alpha", b"1")?;
    }

    // Reader
    {
        let db_ro = Db::open_ro_with_config(root, cfg)?;
        assert_eq!(db_ro.get(b"alpha")?.as_deref(), Some(b"1".as_ref()));
    }

    Ok(())
}
```

Notes:
- With data_fsync=false, durability relies on WAL only; WAL is not truncated on commit and will truncate during replay after an unclean shutdown.
- Page cache is per-Pager instance; hits/misses recorded in metrics.

---

## 4) Crash-safety and commit contract

Commit sequence for a page write:
1. Compute new LSN = meta.last_lsn + 1.
2. Snapshot COW: if there are active snapshots that may need the current page (snapshot_lsn ≥ current_page_lsn), freeze the current page bytes before overwrite (append to freeze.bin and index.bin).
3. If page is v2, write lsn into header and update page CRC.
4. Append WAL record (header + payload), fsync WAL (group-commit).
5. Write data page to its segment file (fsync controlled by data_fsync).
6. Maybe rotate (truncate) WAL — only when data_fsync is enabled.
7. Update meta.last_lsn.
8. Update page cache entry (if enabled).

Additional COW cases:
- Freeing overflow chains: freeze each OVERFLOW page before free if any snapshot may need it.
- Removing empty KV pages from chains: freeze the KV page before free if any snapshot may need it.

Replay (writer only):
- If meta.clean_shutdown == false, read WAL frames (CRC + length + partial tail tolerant).
- Ignore unknown record types (forward-compatible).
- Apply page images with LSN-gating for v2 pages (KV_RH/OVERFLOW).
- Truncate WAL to header after success; update last_lsn best-effort.

Partial tails:
- A short read for a record header or payload is treated as normal EOF.

Optimization (v1.2):
- WAL replay performs LSN-gating before ensure_allocated to avoid unnecessary allocations.

---

## 5) Rust API: Db (high-level)

Core operations:
- Db::open / Db::open_with_config (writer; exclusive lock)
- Db::open_ro / Db::open_ro_with_config (reader; shared lock)
- put(key, val)
- get(key)
- del(key)
- print_stats()

Scanning:
- scan_all() -> Vec<(Vec<u8>, Vec<u8>)>
- scan_prefix(prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>
  - Collects KV pairs across all buckets/chains.
  - Tail-wins semantics: last occurrence along a chain wins.
  - Return order is unspecified.

Subscriptions (in-process):
- subscribe_prefix(prefix, callback) -> SubscriptionHandle
  - Synchronous callbacks on writer thread after successful commit.
  - Event: { key: Vec<u8>, value: Option<Vec<u8>>, lsn: u64 }.

Snapshot Isolation (Phase 1):
- Db::snapshot_begin() -> Result<SnapshotHandle>
  - SnapshotHandle:
    - lsn() -> u64
    - get(key: &[u8]) -> Result<Option<Vec<u8>>> — consistent at snapshot_lsn
    - scan_all()/scan_prefix() — consistent at snapshot_lsn
    - end() — drop sidecar (freeze/index)
- Details:
  - Snapshot reads choose live vs frozen page for each page_id (as-of snapshot LSN).
  - Overflow chains resolved per-page the same way (as-of snapshot).
  - Fallback scan (rare) reconstructs tail-wins if chain head moved after snapshot.

Backup/Restore (Phase 1):
- backup::backup_to_dir(&db, &snap, out_dir, since_lsn: Option<u64>)
  - Full: since_lsn=None, includes all pages as-of snapshot_lsn.
  - Incremental: since_lsn=Some(N), includes pages with page_lsn ∈ (N, S].
  - Output: pages.bin (frames), dir.bin (optional), manifest.json (summary).
  - Retries index refresh for frozen pages (hot backup robustness).
- backup::restore_from_dir(dst_root, backup_dir)
  - Writes pages to dst_root, installs dir.bin if present, updates last_lsn and clean_shutdown.

Page cache:
- Enabled when page_cache_pages > 0.
- Metrics track hits/misses.

---

## 6) Rust API: Pager (low-level)

For tooling/testing (bypasses the KV layer). Use with care.

- Pager::open(root) / Pager::open_with_config(root, &cfg)
- allocate_pages(n) -> start_id
- allocate_one_page() -> page_id (reuses from free-list when possible)
- ensure_allocated(page_id) — ensure underlying segment exists and covers the page
- write_page_raw(page_id, &buf) — writes page without WAL (used by replay/restore)
- commit_page(page_id, &mut buf) — WAL + fsync (commit sequence above)
- read_page(page_id, &mut buf)

Page cache:
- Enabled by config/env; page_cache_pages > 0.
- Metrics report hits/misses.

---

## 7) Directory and free-list

Directory:
- Directory::create(root, buckets) — creates the dir file with heads=NO_PAGE and header CRC.
- Directory::open(root) — validates magic/version and CRC.
- head(bucket) -> page_id and set_head(bucket, page_id) — atomic (tmp+rename), CRC updated.

Free-list:
- File <root>/free with magic/version and appended u64 page IDs.
- FreeList::open, push(page_id), pop() -> Option<u64>, count().
- count() derived from file length; header count is best-effort.

---

## 8) WAL (advanced)

Most users should not use WAL directly. Advanced APIs:
- wal::Wal::open_for_append(root)
- wal::Wal::append_page_image(lsn, page_id, payload)
- wal::Wal::fsync()
- wal::Wal::maybe_truncate()
- wal_replay_if_any(root) — writer-only startup recovery

CDC tooling (see docs/cdc.md):
- wal-tail — JSONL frames (type/lsn/page_id/len/crc32)
- wal-ship — wire stream (header + frames), TCP/file/stdout sinks, compression (gzip/zstd), since-lsn, batching
- wal-apply — idempotent apply with LSN-gating
- record/replay — deterministic ranges by LSN
- last-lsn — follower checkpoint helper

Wire format (v1):
- Header (16): "P1WAL001" + reserved u32 + reserved u32
- Record (28): [type u8][flags u8][reserved u16][lsn u64][page_id u64][len u32][crc32 u32]
- Payload: full page image (PAGE_IMAGE), empty for TRUNCATE in streaming ship
- Partial tails tolerated; unknown record types ignored

---

## 9) Environment variables

Core:
- P1_WAL_COALESCE_MS = integer (default 3) — WAL fsync coalescing (ms)
- P1_DATA_FSYNC = 0|1 (default 1) — fsync data segments per commit
  - When 0: WAL is not truncated at commit; truncated on replay.
- P1_PAGE_CACHE_PAGES = N — read cache size (0 disables)
- P1_OVF_THRESHOLD_BYTES = N — overflow threshold (default: page_size/4)

RH compaction heuristics:
- P1_RH_COMPACT_DEAD_FRAC, P1_RH_COMPACT_MIN_BYTES

CDC (ship/apply):
- P1_SHIP_FLUSH_EVERY, P1_SHIP_FLUSH_BYTES
- P1_SHIP_SINCE_INCLUSIVE = 1|true|yes|on
- P1_SHIP_COMPRESS = none|gzip|zstd
- P1_APPLY_DECOMPRESS = none|gzip|zstd

CLI helper:
- P1_DB_GET_OUT = path — write raw value to a file in dbget

---

## 10) Error handling

- APIs return anyhow::Result.
- WAL replay skips partial frames and treats CRC mismatches at the tail as normal EOF.
- check --strict exits with error on directory failure, CRC/IO issues, or overflow orphans.

---

## 11) CLI ↔ API mapping

Admin/low-level:
- init (init_db), status (meta/dir/free summary), alloc/read/write (Pager)
- free-status/free-pop/free-push (FreeList)
- RH ops: pagefmt-rh, rh-put, rh-get, rh-list, rh-del, rh-compact

High-level DB:
- dbinit (Directory::create)
- dbput/dbget/dbdel/dbstats (Db)
- dbscan [--prefix ...] [--json] (Db::scan_all/scan_prefix)

Snapshots/Backup:
- backup --path ./db --out ./backup [--since-lsn N]
- restore --path ./dst --from ./backup [--verify]
  - verify runs strict check after restore

Integrity/maintenance:
- check [--strict] [--json], repair
- metrics [--json], metrics-reset

CDC:
- wal-tail [--follow]
- wal-ship [--follow] [--since-lsn N] [--sink=tcp://host:port|file://path]
- wal-apply --path ./follower (reads stdin)
- cdc last-lsn --path
- cdc record --path --out file [--from-lsn X] [--to-lsn Y]
- cdc replay --path [--input file] [--from-lsn X] [--to-lsn Y]

---

## 12) Platform notes

- Windows: fsync of directory entries after rename is best-effort. On Unix, parent directory fsync is performed.
- Endianness: all integers on disk are little-endian.
- Locks: fs2 advisory locks, file <root>/LOCK.

---

## 13) On-disk formats (summary)

Meta (v3) — <root>/meta:
- Magic: P1DBMETA
- Fields (LE): version u32=3, page_size u32, flags u32, next_page_id u64, hash_kind u32, last_lsn u64, clean_shutdown u8
- Written via temp file + rename; parent dir fsync (best-effort on Windows)

Page (v2) — size = page_size:
- 64-byte header:
  - MAGIC4="P1PG", version u16=2, type u16 (KV_RH=2 | OVERFLOW=3), pad u32
  - page_id u64, data_start u16, table_slots u16, used_slots u16, flags u16
  - next_page_id u64, lsn u64, seed u64, crc32 u32
- CRC computed over the whole page with the CRC field zeroed

KV_RH (2):
- Records: [klen u16][vlen u16][key][value]
- Slot table at the end: [off u16][fp u8][dist u8] (Robin Hood probing)

OVERFLOW (3):
- Data chunk length in data_start (u16), chains via next_page_id
- KV entry stores an 18-byte placeholder: [0xFF][0][total_len u64][head_pid u64]

WAL (v1) — <root>/wal-000001.log:
- Header (16): P1WAL001 + reserved u32 + reserved u32
- Record header (28): type u8, flags u8, reserved u16, lsn u64, page_id u64, len u32, crc32 u32
- Payload: full page image (PAGE_IMAGE). TRUNCATE (streaming) has empty payload.
- Replay rules: CRC verify, stop on partial tail, ignore unknown types, LSN-gate for v2 pages, truncate to header after success.

---

## 14) Practical examples

Initialize, create directory, basic ops:

```bash
quiverdb init --path ./db --page-size 4096
quiverdb dbinit --path ./db --page-size 4096 --buckets 128
quiverdb dbput --path ./db --key alpha --value 1
quiverdb dbget --path ./db --key alpha
quiverdb dbdel --path ./db --key alpha
quiverdb dbstats --path ./db
```

Scan and CDC snippets:

```bash
# Scan all / by prefix (JSON)
quiverdb dbscan --path ./db --json
quiverdb dbscan --path ./db --prefix k --json

# Ship WAL since follower LSN to TCP, follow
LSN=$(quiverdb cdc last-lsn --path ./follower)
quiverdb wal-ship --path ./leader --since-lsn $LSN --sink=tcp://127.0.0.1:9999 --follow

# Record/replay deterministic slice
quiverdb cdc record --path ./leader --out ./slice.bin --from-lsn 100 --to-lsn 200
quiverdb cdc replay --path ./follower --input ./slice.bin --from-lsn 100 --to-lsn 200
```

Snapshots/Backup:

```bash
# Full backup at snapshot
quiverdb backup --path ./db --out ./backup

# Incremental backup
quiverdb backup --path ./db --out ./backup_incr --since-lsn 12345

# Restore (optional verify)
quiverdb restore --path ./db_restored --from ./backup --verify
```

Rust (snapshot isolation):

```rust
use anyhow::Result;
use QuiverDB::{Db, Directory, init_db};

fn main() -> Result<()> {
    let root = std::path::Path::new("./db");
    if !root.exists() {
        init_db(root, 4096)?;
        Directory::create(root, 128)?;
    }

    let mut db = Db::open(root)?;
    db.put(b"k", b"v0")?;

    let mut s = db.snapshot_begin()?;
    db.put(b"k", b"v1")?;

    assert_eq!(s.get(b"k")?.as_deref(), Some(b"v0".as_ref()));
    db.snapshot_end(&mut s)?;
    Ok(())
}
```

---

This API guide summarizes v1.2 Phase 1: in-process snapshot isolation and backup/restore without changing frozen on-disk formats, plus the existing WAL/CDC, directory, and KV APIs.