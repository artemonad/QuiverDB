# QuiverDB
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GitHub release](https://img.shields.io/github/v/release/artemonad/QuiverDB)](https://github.com/artemonad/QuiverDB/releases)
[![Docs](https://img.shields.io/badge/docs-API%20%2F%20Format%20%2F%20CDC-informational)](docs/)
[![GitHub stars](https://img.shields.io/github/stars/artemonad/QuiverDB?style=social)](https://github.com/artemonad/QuiverDB/stargazers)

Embedded Rust key–value database with in-page Robin Hood indexing, WAL, a CRC-validated directory of bucket heads, and overflow chains for large values.

Status: v1.2 (Phase 1) + Phase 2 features (snapstore dedup and persisted snapshots), with frozen on-disk formats:
- meta v3, page v2, WAL v1 (no migrations, forward/backward compatible)
- Phase 1 snapshots are in-process by default; Phase 2 adds optional persisted snapshots and a shared dedup store (snapstore)

## Highlights

- Frozen on-disk formats
  - Meta v3, Page v2 (64-byte header, per-page LSN, whole-page CRC), WAL v1
  - No format migrations across 1.x

- Crash safety with WAL
  - Full page images, CRC, monotonic LSN
  - Replay tolerates partial tails and ignores unknown record types (forward-compatible)
  - LSN-gating: v2 pages (KV_RH/OVERFLOW) only apply when wal_lsn > page_lsn

- Single writer, multiple readers
  - Writer: exclusive lock; replays WAL if needed; sets clean_shutdown=false on open → true on drop
  - Readers: shared lock; never replay WAL; do not modify meta; safe concurrent reads

- KV with in-page Robin Hood index
  - Insert/update/delete/list/compact and rebuild at page level
  - Large values stored in overflow chains referenced by a compact placeholder

- Directory and free-list
  - Directory heads (per bucket) with CRC and atomic tmp+rename updates
  - Free-list file for page reuse

- CDC (Change Data Capture)
  - wal-ship (stdout/TCP/file, since-lsn), wal-apply (idempotent, LSN-gating), wal-tail (JSONL)
  - Deterministic record/replay with LSN filters
  - Optional compression (gzip/zstd) for the wire stream

- Snapshot Isolation (Phase 1) + Backup/Restore
  - In-process snapshots “as of LSN” with page-level copy-on-write
  - Full and incremental backup (since_lsn), robust restore, CRC-checked archives
  - Rare fallback scans reconstruct tail-wins when hot chain mutations occur after the snapshot

- Phase 2: Snapstore (dedup) and persisted snapshots
  - Content-addressed store for frozen page images with refcounting and compact
  - Persisted snapshot registry (.snapshots/registry.json) with CLI to list/remove
  - Snapshot readers can fallback to snapstore via hashindex.bin if sidecar freeze.bin is missing

- Developer comfort
  - Config builder (QuiverConfig/DbBuilder) + env compatibility
  - In-process subscriptions: subscribe_prefix(prefix, callback)
  - Scan APIs: scan_all(), scan_prefix(prefix)
  - Metrics, check/repair, JSON modes in CLI
  - Minimal C FFI (qdb_init_db/open_ro/get/put/del)
  - Python bindings via PyO3 (minimal wrapper)

## Contents

- Quick start (CLI)
- Quick start (Rust API)
- Snapshot Isolation and Backup/Restore (Phase 1 + Phase 2 dedup)
- CDC overview
- Configuration (env and builder)
- CLI cheat sheet
- Environment variables
- On-disk formats (summary)
- Platform notes
- License

## Quick start (CLI)

Initialize a new DB and directory:
```bash
quiverdb init   --path ./db --page-size 4096
quiverdb dbinit --path ./db --page-size 4096 --buckets 128
```

Basic KV:
```bash
quiverdb dbput --path ./db --key alpha --value 1
quiverdb dbget --path ./db --key alpha
quiverdb dbdel --path ./db --key alpha
quiverdb dbstats --path ./db
```

Scan (all / by prefix), optionally JSON:
```bash
quiverdb dbscan --path ./db --json
quiverdb dbscan --path ./db --prefix k --json
```

Integrity and metrics:
```bash
quiverdb check --path ./db --strict --json
quiverdb repair --path ./db
quiverdb metrics --json
quiverdb metrics-reset
```

Backup/Restore (Phase 1):
```bash
# Full backup
quiverdb backup --path ./db --out ./backup

# Incremental backup (pages with page_lsn in (since, snapshot])
quiverdb backup --path ./db --out ./backup_incr --since-lsn 12345

# Restore (optional strict verification after restore)
quiverdb restore --path ./db_restored --from ./backup --verify
```

Snapshots (Phase 2: persisted snapshots + snapstore)
```bash
# List persisted snapshots (from .snapshots/registry.json)
quiverdb snapshots --path ./db [--json]

# Remove a persisted snapshot: dec_ref in snapstore + delete sidecar, mark ended in registry
quiverdb snaprm --path ./db --id <snapshot_id>

# Compact snapstore: rewrite store.bin to keep only frames with refcnt > 0
quiverdb snapcompact --path ./db [--json]
```

WAL checkpoint (manual)
```bash
# Truncate WAL to header (requires data_fsync=true)
quiverdb checkpoint --path ./db
```

CDC (local pipe, TCP, and deterministic slices):
```bash
# Local pipe
quiverdb wal-ship --path ./leader --follow | quiverdb wal-apply --path ./follower

# Resume by follower’s last LSN over TCP
LSN=$(quiverdb cdc last-lsn --path ./follower)
quiverdb wal-ship --path ./leader --since-lsn $LSN --sink=tcp://127.0.0.1:9999 --follow
# On follower:
nc -l -p 9999 | quiverdb wal-apply --path ./follower

# Deterministic record/replay
quiverdb cdc record --path ./leader --out ./slice.bin --from-lsn 100 --to-lsn 200
quiverdb cdc replay --path ./follower --input ./slice.bin --from-lsn 100 --to-lsn 200
```

Tip:
- Some commands also support JSON via flags (e.g., dbstats --json) or env toggles (see Environment variables).

## Quick start (Rust API)

Using the config builder and explicit open_with_config:
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

Subscriptions and scans:
```rust
use std::sync::{Arc, Mutex};
use QuiverDB::{Db, Directory, init_db};
use QuiverDB::subs::Event;

fn main() -> anyhow::Result<()> {
    let root = std::path::Path::new("./db");
    if !root.exists() { init_db(root, 4096)?; Directory::create(root, 128)?; }

    let mut db = Db::open(root)?;

    // In-process subscription for keys starting with "k"
    let events: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
    let evs = events.clone();
    let _h = db.subscribe_prefix(b"k".to_vec(), move |ev: &Event| {
        evs.lock().unwrap().push(ev.clone());
    });

    db.put(b"k123", b"v1")?;
    db.del(b"k123")?;

    // Scans (tail-wins within a chain)
    let pairs = db.scan_prefix(b"k")?;
    Ok(())
}
```

## Snapshot Isolation and Backup/Restore (Phase 1 + Phase 2)

- Phase 1: in-process snapshots capture a consistent view at a specific LSN (snapshot_lsn)
  - SnapshotHandle::get/scan_* read “as of LSN”
  - Page-level copy-on-write: before overwriting/freeing a v2 page whose LSN may be needed by a live snapshot, the current image is frozen into a sidecar store
  - Readers choose live vs frozen per page; overflow chains are resolved per page the same way
  - Rarely, a fallback scan reconstructs tail-wins when hot chain mutations occur after the snapshot

- Backup/Restore built on top of snapshots
  - Full: all page images as-of snapshot_lsn
  - Incremental: only pages with page_lsn in (since_lsn, snapshot_lsn]
  - Archive layout:
    - pages.bin — concatenated frames: [page_id u64][page_lsn u64][page_len u32][crc32 u32] + payload(page_size)
    - dir.bin — copy of the directory (if present)
    - manifest.json — summary (snapshot_lsn, since_lsn, page_size, counters)
  - Restore writes pages back (raw), installs dir.bin if present, sets last_lsn and clean_shutdown

- Phase 2: persisted snapshots and snapstore (dedup)
  - Persisted snapshot registry under <root>/.snapshots/registry.json (best-effort)
  - Snapstore is a content-addressed store under <root>/.snapstore by default (configurable via P1_SNAPSTORE_DIR)
    - store.bin stores frames [hash u64][len u32][crc32 u32] + payload
    - index.bin maintains a full mapping hash → (offset, refcnt)
  - Each sidecar also writes hashindex.bin with [page_id u64][hash u64][page_lsn u64]
  - Snapshot readers can fallback to snapstore by hash when sidecar freeze.bin is missing
  - Refcounting ensures frames are retained until all referencing snapshots are removed; snapcompact reclaims space

Notes:
- Formats remain frozen: meta v3, page v2, WAL v1
- Phase 1 snapshots are in-process by default; Phase 2 makes them manageable/persistable and deduplicated
- Snapstore directory can be customized via env (see below)

## CDC overview

- wal-ship
  - Streams a binary WAL header + records to stdout/TCP/file
  - since-lsn filter on the leader (default exclusive > N; an inclusive variant via env)
  - Optional compression: gzip/zstd applied to the entire wire stream

- wal-apply
  - Reads a WAL wire stream from stdin (optionally with decompression)
  - Verifies CRC, tolerates partial tails, ignores unknown types, and applies v2 page images only when wal_lsn > page_lsn (idempotent)

- wal-tail
  - JSONL view of WAL frames for observability; prints “truncate” events on rotations

- cdc record / replay
  - Save a deterministic slice of the WAL wire stream to a file (LSN filters)
  - Reapply it idempotently (optionally with additional LSN bounds)

- last-lsn
  - Prints meta.last_lsn — useful to resume shipping from followers

## Configuration (env and builder)

Builder (preferred inside code):
- QuiverConfig/DbBuilder with:
  - wal_coalesce_ms (group-commit fsync window; default 3 ms)
  - data_fsync (fsync data segments per commit; default true)
  - page_cache_pages (read cache size; default 0 = disabled)
  - ovf_threshold_bytes (None = page_size/4)
  - snap_persist (enable persisted snapshots; default false)
  - snap_dedup (enable content-addressed dedup; default false)
  - snapstore_dir (optional custom directory for snapstore; default <root>/.snapstore)

Environment variables (backward-compatible + Phase 2):
- Core
  - P1_WAL_COALESCE_MS
  - P1_DATA_FSYNC=0|1
  - P1_PAGE_CACHE_PAGES=N
  - P1_OVF_THRESHOLD_BYTES=N
- Robin Hood heuristics
  - P1_RH_COMPACT_DEAD_FRAC, P1_RH_COMPACT_MIN_BYTES
- CDC
  - P1_SHIP_FLUSH_EVERY, P1_SHIP_FLUSH_BYTES
  - P1_SHIP_SINCE_INCLUSIVE=1|true|yes|on
  - P1_SHIP_COMPRESS=none|gzip|zstd
  - P1_APPLY_DECOMPRESS=none|gzip|zstd
- Snapshots / Snapstore (Phase 2)
  - P1_SNAP_PERSIST=0|1 — keep sidecar and track in registry.json
  - P1_SNAP_DEDUP=0|1 — enable snapstore dedup and hashindex
  - P1_SNAPSTORE_DIR=path — custom snapstore directory:
    - absolute path used as-is,
    - relative path resolved against DB root (<root>/<path>)

Builder takes precedence when you use open_with_config/open_ro_with_config for core tunables; snapstore directory is resolved via env at the moment.

## CLI cheat sheet

- High-level DB
  - dbinit, dbput, dbget, dbdel, dbstats
  - dbscan [--prefix ...] [--json]
- Snapshots/Backup (Phase 1)
  - backup --path ./db --out ./backup [--since-lsn N]
  - restore --path ./db_restored --from ./backup [--verify]
- CDC
  - wal-tail [--follow]
  - wal-ship [--follow] [--since-lsn N] [--sink=tcp://host:port|file://path]
  - wal-apply --path ./follower (reads stdin)
  - cdc last-lsn --path
  - cdc record --path --out file [--from-lsn X] [--to-lsn Y]
  - cdc replay --path [--input file] [--from-lsn X] [--to-lsn Y]
- Integrity/Maintenance
  - check [--strict] [--json], repair
  - metrics [--json], metrics-reset
- Snapshots (Phase 2)
  - snapshots --path ./db [--json]
  - snaprm --path ./db --id <snapshot_id>
  - snapcompact --path ./db [--json]
- WAL
  - checkpoint --path ./db (truncate WAL to header; requires data_fsync=true)
- Admin/low-level
  - init, status, alloc, read, write
  - free-status, free-pop, free-push
  - Robin Hood page ops: pagefmt-rh, rh-put, rh-get, rh-list, rh-del, rh-compact

## Environment variables

- P1_WAL_COALESCE_MS=ms — group-commit fsync coalescing window (default 3)
- P1_DATA_FSYNC=0|1 — fsync data segments per commit (default 1; when 0, WAL is only truncated on replay)
- P1_PAGE_CACHE_PAGES=N — page cache size (0 disables)
- P1_OVF_THRESHOLD_BYTES=N — overflow threshold (default page_size/4)
- P1_RH_COMPACT_DEAD_FRAC, P1_RH_COMPACT_MIN_BYTES — RH page compaction heuristics
- P1_SHIP_FLUSH_EVERY, P1_SHIP_FLUSH_BYTES — CDC ship batching
- P1_SHIP_SINCE_INCLUSIVE=1 — CDC since-lsn inclusive mode
- P1_SHIP_COMPRESS=none|gzip|zstd — CDC ship compression
- P1_APPLY_DECOMPRESS=none|gzip|zstd — CDC apply decompression
- P1_DB_GET_OUT=path — write dbget payload to a file
- P1_SNAP_PERSIST=0|1 — persisted snapshots (keep sidecar + registry)
- P1_SNAP_DEDUP=0|1 — snapstore dedup (hashindex + refcount)
- P1_SNAPSTORE_DIR=path — custom snapstore directory (absolute or relative to <root>)

## On-disk formats (summary)

- Meta (v3)
  - MAGIC “P1DBMETA”
  - version u32=3, page_size u32, flags u32, next_page_id u64, hash_kind u32, last_lsn u64, clean_shutdown u8
  - Written via tmp+rename; parent dir fsync on Unix (best-effort on Windows)

- Page (v2)
  - 64-byte header:
    - MAGIC “P1PG”, version=2, type (KV_RH=2 | OVERFLOW=3)
    - page_id u64, data_start u16, table_slots u16, used_slots u16, flags u16
    - next_page_id u64, lsn u64, seed u64, crc32 u32
  - Whole-page CRC with its field zeroed during calculation

- WAL (v1)
  - Header (16B): “P1WAL001” + reserved u32 + reserved u32
  - Record (28B): [type u8][flags u8][reserved u16][lsn u64][page_id u64][len u32][crc32 u32]
  - Payload: full page image for PAGE_IMAGE; empty for TRUNCATE (streaming ship)
  - Replay: CRC verify; stop on partial tail; ignore unknown types; LSN-gating for v2 pages

All integers are Little Endian (LE). See docs/format.md for details.

## Platform notes

- Locks: fs2 advisory locks using <root>/LOCK
- Windows: directory fsync after rename is best-effort (enabled on Unix)
- Endianness: on-disk integers are little-endian

## Build / Install

- CLI (Release):
```bash
cargo build --release
```
Binary path: target/release/quiverdb

- Library usage (Cargo.toml):
```toml
[dependencies]
QuiverDB = { path = "./QuiverDB" } # or your git path
```

- FFI (C ABI): see docs/ffi.md
  - Build with feature “ffi”: cdylib + cbindgen header generation

## License

MIT — see LICENSE.

Contributions, issues, and feature requests are welcome. Check docs/api.md, docs/cdc.md, docs/snapshots.md for details and examples.