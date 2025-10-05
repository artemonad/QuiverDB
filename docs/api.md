# QuiverDB API Guide (v1.1.0)

This document describes the developer-facing API for QuiverDB: Rust interfaces (Db/Pager/Directory), crash-safety contract, locking semantics, configuration (QuiverConfig/DbBuilder), subscriptions, scan APIs, environment variables, and CLI mapping.

Audience: Rust developers embedding QuiverDB or building tooling around it.

Status: v1.1.0 (formats frozen: meta v3, page v2, WAL v1)


## 1) Concepts

- Single-writer, multi-reader:
  - Writer opens with an exclusive lock and may modify files.
  - Readers open with a shared lock and must not change files.
- Crash safety via WAL:
  - All changes are appended to WAL (CRC-protected, monotonic LSN).
  - Replay applies page images with LSN-gating (apply only if `wal_lsn > page_lsn` on v2 pages).
  - On clean shutdown: WAL is truncated to header (fast start).
- Pages:
  - v2 64-byte header with page LSN and CRC, types: KV_RH (Robin Hood) and OVERFLOW.
  - KV_RH stores key/value pairs in-page with a Robin Hood index.
  - OVERFLOW stores large values in chains referenced by placeholders from KV pages.
- Directory:
  - Per-bucket heads — forms chains of KV pages (head → next → …).
  - Atomic updates (tmp+rename) + header CRC.
- Free-list:
  - Tracks freed page IDs for reuse.
- Tail-wins within a chain:
  - If multiple versions of a key exist along a bucket chain, the last one (closer to chain tail) is authoritative for reads and scans.


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

Lock file path: `<root>/LOCK` (fs2 advisory locks). On Windows, semantics are best-effort but guarded by tests.


## 3) Configuration (v1.1)

QuiverDB now has an explicit configuration object with a builder, while remaining backward-compatible with environment variables.

- QuiverConfig fields:
  - wal_coalesce_ms: u64 — WAL fsync coalescing (ms), default 3
  - data_fsync: bool — fsync data segments on commit, default true
  - page_cache_pages: usize — page cache size (0 disables), default 0
  - ovf_threshold_bytes: Option<usize> — overflow threshold; None → page_size/4

- Sources and precedence:
  - By default, `Db::open` and `Db::open_ro` read env vars into a QuiverConfig (keeping old behavior).
  - You can pass an explicit config via `Db::open_with_config` / `Db::open_ro_with_config`. Explicit config takes precedence over env.

Example (builder + writer/reader):

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

    // Build config (starts from env by default, then override)
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
        let v = db_ro.get(b"alpha")?.unwrap();
        assert_eq!(v, b"1");
    }

    Ok(())
}
```

Notes:
- When `data_fsync=false`, durability relies on WAL; WAL is not truncated on commit and will truncate during replay after an unclean shutdown.
- Page cache is per-Pager instance; hits/misses are tracked in metrics.


## 4) Crash-safety and commit contract

Commit sequence for a page write:
1. Compute new LSN = `meta.last_lsn + 1`.
2. If page is v2, write `lsn` into header and update page CRC.
3. Append WAL record (header + payload), fsync WAL (group-commit).
4. Write data page to its segment file (fsync controlled by `data_fsync`).
5. Optionally rotate (truncate) WAL — only when data fsync is enabled.
6. Update `meta.last_lsn`.
7. Update page cache entry (if enabled).

Replay (writer only):
- If `meta.clean_shutdown == false`, read WAL frames (CRC + length + partial tail tolerant).
- Ignore unknown record types (forward-compatible).
- Apply page images with LSN-gating for v2 pages (KV_RH/OVERFLOW).
- Truncate WAL to header after success; update `last_lsn` best-effort.

Partial tails:
- A short read for a record header or payload is treated as normal EOF (tail still being written), not an error.


## 5) Rust API: Db (high-level)

Core operations:

- `Db::open` (exclusive; from env)
- `Db::open_with_config` (exclusive; explicit config)
- `Db::open_ro` (shared; from env)
- `Db::open_ro_with_config` (shared; explicit config)
- `put(key, val)` — insert/update; may use overflow chains for large values
- `get(key)` — resolves overflow chains transparently
- `del(key)` — deletes key and frees its overflow chain if present
- `print_stats()` — prints DB stats and a metrics snapshot (human-friendly)

Scanning (v1.1):
- `scan_all() -> Vec<(Vec<u8>, Vec<u8>)>`
- `scan_prefix(prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>`
  - Collects KV pairs across all buckets/chains.
  - Tail-wins semantics: last occurrence along a chain overrides earlier ones.
  - Return order is unspecified (use your own sorting if needed).

Subscriptions (v1.1):
- `subscribe_prefix(prefix, callback) -> SubscriptionHandle`
  - In-process pub/sub for live events emitted by the writer right after a successful commit.
  - Event: `{ key: Vec<u8>, value: Option<Vec<u8>>, lsn: u64 }`
    - `value=Some(...)` for put/update, `None` for delete.
  - Drop the SubscriptionHandle to unsubscribe.
  - Callbacks run synchronously in the writer thread; keep handlers fast, offload heavy work to another thread if needed.

Example:

```rust
use std::sync::{Arc, Mutex};
use QuiverDB::{Db, Directory, init_db};
use QuiverDB::subs::Event;

fn main() -> anyhow::Result<()> {
    let root = std::path::Path::new("./db");
    if !root.exists() {
        init_db(root, 4096)?;
        Directory::create(root, 128)?;
    }

    let mut db = Db::open(root)?;
    let events: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
    let evs = events.clone();

    let _h = db.subscribe_prefix(b"k".to_vec(), move |ev: &Event| {
        evs.lock().unwrap().push(ev.clone());
    });

    db.put(b"k1", b"v1")?;
    db.del(b"k1")?;

    assert_eq!(events.lock().unwrap().len(), 2);
    Ok(())
}
```

Notes:
- In bucket chains, the newest copy is the last encountered (tail). Db::get and scans choose the last match to avoid stale reads if earlier pages were updated for other keys.


## 6) Rust API: Pager (low-level)

For low-level tooling/testing (bypasses the KV layer). Use with care.

- `Pager::open(root) -> Pager` (from env)
- `Pager::open_with_config(root, &cfg) -> Pager` (explicit conf)
- `allocate_pages(n) -> start_id`
- `allocate_one_page() -> page_id` (prefers reusing from free-list)
- `ensure_allocated(page_id)` — ensure segment exists and length covers the page
- `write_page_raw(page_id, &buf)` — writes page without WAL (used by replay)
- `commit_page(page_id, &mut buf)` — writes via WAL + fsync (commit sequence above)
- `read_page(page_id, &mut buf)`

Example:

```rust
use anyhow::Result;
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{rh_page_init, rh_page_update_crc};

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
- Enabled by config (or env) with `page_cache_pages > 0`.
- Metrics report hits/misses.


## 7) Directory and free-list

Directory:
- `Directory::create(root, buckets)` — creates the directory file with heads=NO_PAGE and header CRC.
- `Directory::open(root)` — validates magic/version and CRC.
- `head(bucket) -> page_id` and updates via `set_head(bucket, page_id)` are atomic (tmp+rename). Header stores CRC (computed over `[version u32][buckets u32] + heads bytes`).

Free-list:
- File `<root>/free` with magic/version and appended `u64` page IDs.
- `FreeList::open`, `push(page_id)`, `pop() -> Option<u64>`, `count()`.
- `count()` is computed from file length; header `count` is best-effort.


## 8) WAL (advanced)

Most users should not use WAL directly (Db/Pager handle it). Advanced APIs:
- `wal::Wal::open_for_append(root)`
- `wal::Wal::append_page_image(lsn, page_id, payload)`
- `wal::Wal::fsync()`, `wal::Wal::maybe_truncate()`
- `wal_replay_if_any(root)` — writer-only startup recovery

CDC tooling (see docs/cdc.md for full guide):
- `wal-tail` prints JSONL frames (type/lsn/page_id/len/crc32).
- `wal-ship` emits the raw binary stream (WAL header + records); handles mid-stream header/truncate.
  - v1.1: `--sink=tcp://host:port` and `--since-lsn N` (filter frames with lsn > N).
- `wal-apply` consumes a WAL stream and applies page images idempotently (LSN-gated for v2).
- `cdc record` stores a slice of the wire-format WAL to a file with LSN filters.
- `cdc replay` replays from file (or stdin) with optional LSN filters.
- `cdc last-lsn` prints follower’s last LSN for resume.


## 9) Environment variables

- `P1_WAL_COALESCE_MS=` integer (default 3) — WAL fsync coalescing delay (ms).
- `P1_DATA_FSYNC=0|1` (default 1) — fsync data segments per commit:
  - When `0`: rely solely on WAL durability; WAL is not truncated at commit and will truncate during replay.
- `P1_PAGE_CACHE_PAGES=N` — number of pages in the read cache (0 disables).
- `P1_OVF_THRESHOLD_BYTES=N` — value size threshold for overflow (default: page_size/4).
- `P1_RH_COMPACT_DEAD_FRAC`, `P1_RH_COMPACT_MIN_BYTES` — RH auto-compaction heuristics.
- `P1_DB_GET_OUT=path` — raw value output file for `dbget`.

Note: Using `Db::open_with_config`/`Db::open_ro_with_config` lets you override env-driven defaults.


## 10) Error handling

- APIs return `anyhow::Result`; errors include I/O and logical validation failures (CRC mismatch, invalid magic).
- WAL replay skips partial frames or CRC mismatches at the tail (treated as normal EOF).
- `check --strict` exits with error on directory failure, CRC/IO issues, or overflow orphans.


## 11) CLI ↔ API mapping

- Admin/low-level:
  - `init` (init_db), `status` (meta/dir/free summary), `alloc/read/write` (Pager)
  - `free-status/free-pop/free-push` (FreeList)
  - RH page ops: `pagefmt-rh`, `rh-put`, `rh-get`, `rh-list`, `rh-del`, `rh-compact`
- High-level DB:
  - `dbinit` (Directory::create)
  - `dbput/dbget/dbdel/dbstats` (Db)
  - v1.1: `dbscan [--prefix ...] [--json]` (Db::scan_all/scan_prefix)
- Integrity/maintenance:
  - `check [--strict] [--json]`, `repair`
- Metrics:
  - `metrics [--json]`, `metrics-reset`
- CDC:
  - `wal-tail [--follow]`
  - `wal-ship [--follow] [--since-lsn N] [--sink=tcp://host:port]`
  - `wal-apply --path ./follower` (reads from stdin)
  - `cdc last-lsn --path`
  - `cdc record --path --out file [--from-lsn X] [--to-lsn Y]`
  - `cdc replay --path [--input file] [--from-lsn X] [--to-lsn Y]`


## 12) Platform notes

- Windows: fsync of directory entries after `rename` is best-effort. On Unix, parent directory fsync is performed.
- Endianness: all integers on disk are little-endian.


## 13) On-disk formats (summary)

Meta (v3) — `<root>/meta`:
- Magic: `P1DBMETA`
- Fields (LE): `version u32=3`, `page_size u32`, `flags u32`, `next_page_id u64`, `hash_kind u32`, `last_lsn u64`, `clean_shutdown u8`
- Written via temp file + rename; directory fsync (best-effort on Windows).

Page (v2) — size = `page_size` bytes:
- 64-byte header:
  - `MAGIC4="P1PG"`, `version u16=2`, `type u16` (KV_RH=2 | OVERFLOW=3), `pad u32`
  - `page_id u64`, `data_start u16`, `table_slots u16`, `used_slots u16`, `flags u16`
  - `next_page_id u64`, `lsn u64`, `seed u64`, `crc32 u32`
- CRC is computed over the whole page with the CRC field zeroed.

KV_RH (2):
- Records in data area: `[klen u16][vlen u16][key bytes][value bytes]`
- Slot table at the end: array of `[off u16][fp u8][dist u8]` (Robin Hood probing)

OVERFLOW (3):
- Stores chunks of large values in a chain (`next_page_id`).
- KV value stores an 18-byte placeholder: `[0xFF][0][total_len u64][head_pid u64]`

WAL (v1) — `<root>/wal-000001.log`:
- Header (16): `P1WAL001` + reserved u32 + reserved u32
- Record header (28):
  - `type u8` (1=PAGE_IMAGE, 2=TRUNCATE (streaming))
  - `flags u8`, `reserved u16`, `lsn u64`, `page_id u64`, `len u32`, `crc32 u32`
- Payload:
  - PAGE_IMAGE: full page image (`page_size` bytes); TRUNCATE: empty
- Replay rules:
  - Verify CRC, stop on partial tail, ignore unknown types, LSN-gate for v2 pages, truncate to header after success


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

# Deterministic record/replay
quiverdb cdc record --path ./leader --out ./slice.bin --from-lsn 100 --to-lsn 200
quiverdb cdc replay --path ./follower --input ./slice.bin --from-lsn 100 --to-lsn 200
```

Metrics and integrity:

```bash
quiverdb metrics --json
quiverdb check --path ./db --strict --json
quiverdb repair --path ./db
```


---

For replication details, wire formats, and follower apply rules, see docs/cdc.md.