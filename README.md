# QuiverDB
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GitHub release](https://img.shields.io/github/v/release/artemonad/QuiverDB)](https://github.com/artemonad/QuiverDB/releases)
[![Docs](https://img.shields.io/badge/docs-API%20%2F%20Format%20%2F%20CDC-informational)](docs/)
[![GitHub stars](https://img.shields.io/github/stars/artemonad/QuiverDB?style=social)](https://github.com/artemonad/QuiverDB/stargazers)

Embedded Rust key–value database with in-page Robin Hood indexing, write-ahead log (WAL), directory of bucket heads, and overflow chains for large values.

Status: v1.1.0 (formats frozen: meta v3, page v2, WAL v1)

- Frozen on-disk formats (no migrations).
- Crash safety via WAL (CRC, LSN-gating, partial-tail tolerant).
- Single writer, multiple readers (shared lock; readers never mutate).
- Integrity tools (check/repair), metrics, CDC utilities.
- New in 1.1:
  - Config builder (QuiverConfig/DbBuilder), explicit open_with_config.
  - In-process subscriptions (live events on put/del).
  - Scan APIs (scan_all / scan_prefix) + CLI dbscan.
  - CDC extras: wal-ship sink=tcp://…, --since-lsn, deterministic record/replay, last-lsn helper.

Links:
- API guide: docs/api.md
- CDC guide: docs/cdc.md
- On-disk format: docs/format.md


## Highlights

- Page v2 (64-byte header): MAGIC, type, per-page LSN, whole-page CRC.
- In-page Robin Hood index: insert/update/delete/list/compact/rebuild.
- WAL v1: full page images, CRC, monotonic LSN, group-commit (fsync coalescing).
- CDC:
  - wal-tail — JSONL view for observability.
  - wal-ship — binary wire (handles mid-stream header/rotate), now with --since-lsn and tcp:// sink.
  - wal-apply — idempotent apply with LSN-gating.
  - cdc record/replay — deterministic slices with LSN filters; cdc last-lsn for resume.
- Directory: per-bucket heads; atomic tmp+rename with header CRC.
- Overflow pages: chains for large values; free-list to reuse pages; orphan sweep/repair.
- Read-only readers: shared lock, no WAL replay, no meta changes.
- Config builder: override env in code; page cache toggle and size.
- Subscriptions: subscribe_prefix for live events in-process.
- Integrity: check --strict/--json; repair frees orphan overflow chains.


## Install / Build

- CLI (Release):
```
cargo build --release
```
Binary path: target/release/quiverdb

- Library (Cargo.toml):
```toml
[dependencies]
QuiverDB = { path = "./QuiverDB" }  # or your git path
```

Rust version: 2021 edition.


## Quick start (CLI)

Initialize database + directory:
```
quiverdb init --path ./db --page-size 4096
quiverdb dbinit --path ./db --page-size 4096 --buckets 128
```

KV operations:
```
quiverdb dbput  --path ./db --key hello --value world
quiverdb dbget  --path ./db --key hello
quiverdb dbdel  --path ./db --key hello
quiverdb dbstats --path ./db
```

Scan (all / by prefix), optionally JSON:
```
quiverdb dbscan --path ./db --json
quiverdb dbscan --path ./db --prefix k --json
```

Integrity + metrics:
```
quiverdb check --path ./db --strict --json
quiverdb repair --path ./db
quiverdb metrics --json
quiverdb metrics-reset
```

CDC (local pipe or TCP):

- Pipe (local):
```
quiverdb wal-ship --path ./leader --follow | quiverdb wal-apply --path ./follower
```

- TCP + resume by LSN:
```
# Get follower checkpoint
LSN=$(quiverdb cdc last-lsn --path ./follower)

# Ship only frames with lsn > $LSN, to TCP sink
quiverdb wal-ship --path ./leader --since-lsn $LSN --sink=tcp://127.0.0.1:9999 --follow
# On follower:
nc -l -p 9999 | quiverdb wal-apply --path ./follower
```

Deterministic record/replay (wire format):
```
quiverdb cdc record --path ./leader --out ./slice.bin --from-lsn 100 --to-lsn 200
quiverdb cdc replay --path ./follower --input ./slice.bin --from-lsn 100 --to-lsn 200
```


## Quick start (Rust API)

Minimal put/get with explicit config (builder):

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

    // Config: start from env, override desired fields
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

Subscriptions (in-process) and scans:

```rust
use std::sync::{Arc, Mutex};
use QuiverDB::{Db, Directory, init_db};
use QuiverDB::subs::Event;

fn main() -> anyhow::Result<()> {
    let root = std::path::Path::new("./db");
    if !root.exists() { init_db(root, 4096)?; Directory::create(root, 128)?; }

    let mut db = Db::open(root)?;
    // Subscribe to changes for keys starting with "k"
    let events: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(Vec::new()));
    let evs = events.clone();
    let _h = db.subscribe_prefix(b"k".to_vec(), move |ev: &Event| {
        evs.lock().unwrap().push(ev.clone());
    });

    db.put(b"k123", b"v1")?;
    db.del(b"k123")?;

    // Scan by prefix (tail-wins semantics)
    let pairs = db.scan_prefix(b"k")?;
    // pairs: Vec<(key, value)>

    Ok(())
}
```


## Concepts

- Single writer, multiple readers:
  - Writer: exclusive lock; replays WAL if needed; sets clean_shutdown=false on open, true on drop.
  - Reader: shared lock; no WAL replay; never mutates meta; drop does not run space maintenance.
- Crash safety with WAL:
  - All changes are page images appended to WAL (CRC + monotonic LSN).
  - Writer replay is tolerant to partial tails, ignores unknown types, and applies v2 pages only when wal_lsn > page_lsn (LSN-gating).
  - Clean shutdown enables fast start (truncate WAL to header).
- Pages:
  - v2 64-byte header (MAGIC, type KV_RH or OVERFLOW, LSN, CRC).
  - KV_RH stores small values in-page; overflow chains store large values referenced from KV entries via a 18-byte placeholder.
- Directory:
  - Array of bucket heads; KV pages form chains (head → next → …).
  - Updates are atomic via tmp+rename; header includes CRC.
- Free-list:
  - Reuse freed pages; best-effort count in header (truth is file length).
- Tail-wins:
  - If a key is duplicated along a bucket chain, the last entry (closer to tail) is authoritative (Db::get and scans honor this).


## Environment

- P1_WAL_COALESCE_MS (default 3) — fsync coalescing for WAL group-commit (ms).
- P1_DATA_FSYNC=0|1 (default 1) — fsync data segments on commit.
  - When 0: durability relies on WAL; WAL is not truncated on commit (truncated by replay).
- P1_PAGE_CACHE_PAGES=N — read cache size (0 disables).
- P1_OVF_THRESHOLD_BYTES=N — overflow threshold (default page_size/4).
- P1_RH_COMPACT_DEAD_FRAC, P1_RH_COMPACT_MIN_BYTES — RH auto-compaction heuristics.
- P1_DB_GET_OUT=path — redirect dbget value to a file (raw bytes).

Tip: Prefer explicit config via Db::open_with_config/Db::open_ro_with_config (builder overrides env).


## Crash safety (commit and replay)

Commit sequence:
1) LSN = meta.last_lsn + 1
2) For v2, write LSN into page header and update page CRC
3) Append WAL record, fsync WAL (group-commit)
4) Write data page (fsync controlled by data_fsync)
5) Maybe truncate WAL (only when data fsync is enabled)
6) Update meta.last_lsn
7) Update page cache (if enabled)

Replay (writer only; when clean_shutdown=false):
- Read WAL, verify CRC; stop on partial tails.
- Ignore unknown record types (forward-compatible).
- Apply page images only if wal_lsn > page_lsn for v2 pages.
- Truncate WAL back to header after success.


## On-disk formats (frozen)

- Meta v3 (file meta):
  - MAGIC “P1DBMETA”; fields: version u32=3, page_size u32, flags u32, next_page_id u64, hash_kind u32, last_lsn u64, clean_shutdown u8
- Page v2 (size=page_size):
  - 64-byte header:
    - MAGIC “P1PG”, version=2, type (KV_RH=2 | OVERFLOW=3), pad, page_id u64,
      data_start u16, table_slots u16, used_slots u16, flags u16,
      next_page_id u64, lsn u64, seed u64, crc32 u32
- WAL v1:
  - Header (16): “P1WAL001” + reserved u32 + reserved u32
  - Record (28): [type u8][flags u8][reserved u16][lsn u64][page_id u64][len u32][crc32 u32]
  - Payload: full page image (PAGE_IMAGE) or empty (TRUNCATE for streaming ship)

All integers are Little Endian. See docs/format.md for details.


## CLI cheat sheet

Integrity/maintenance:
- check [--strict] [--json]
- repair
- metrics [--json]
- metrics-reset

High-level DB:
- dbinit
- dbput / dbget / dbdel / dbstats
- dbscan [--prefix ...] [--json]

CDC:
- wal-tail [--follow]
- wal-ship [--follow] [--since-lsn N] [--sink=tcp://host:port]
- wal-apply --path ./follower  (reads stdin)
- cdc last-lsn --path
- cdc record --path --out file [--from-lsn X] [--to-lsn Y]
- cdc replay --path [--input file] [--from-lsn X] [--to-lsn Y]

Low-level/admin:
- init, status, alloc, read, write
- free-status / free-pop / free-push
- pagefmt-rh / rh-put / rh-get / rh-list / rh-del / rh-compact


## Platform notes

- Windows: directory fsync after rename is best-effort (on Unix we fsync the parent directory).
- Advisory locks via fs2 (LOCK file).
- Endianness: LE for all on-disk integer fields.


## Roadmap (short)

- 1.2: Snapshot isolation (COW + LSN), incremental backup/restore with dedup.
- 1.3: Observability and packaging (OTel metrics/spans, GitHub Actions, release artifacts).
- See docs/api.md and docs/cdc.md for the most up-to-date API and CDC details.


## License

MIT — see LICENSE.