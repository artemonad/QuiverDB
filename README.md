# QuiverDB
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GitHub release](https://img.shields.io/github/v/release/artemonad/QuiverDB)](https://github.com/artemonad/QuiverDB/releases)
[![Docs](https://img.shields.io/badge/docs-API%20%2F%20Format%20%2F%20CDC-informational)](docs/)
[![GitHub stars](https://img.shields.io/github/stars/artemonad/QuiverDB?style=social)](https://github.com/artemonad/QuiverDB/stargazers)

embedded Rust key–value database
Robin Hood hashing
write-ahead log (WAL)
CDC / replication

An embedded key–value database with in-page Robin Hood indexing, a write-ahead log (WAL), a directory (bucket heads), and overflow chains for large values. It targets single-writer, multi-reader scenarios with crash safety, compact on-disk formats, and simple operations.

Status: v1.0.0 (formats frozen)
- meta v3, page v2 (RH/Overflow), WAL v1 — frozen and stable
- Crash safety via WAL (CRC, LSN-gating, tolerant to partial tails)
- Read-only readers (shared lock) without replay or meta mutations
- Integrity tooling (check/repair with strict mode), metrics, CDC utilities

## Highlights

- Page v2 (64-byte header): MAGIC, type, LSN, whole-page CRC
- In-page Robin Hood index (slots + data area): insert/update/delete/list/compact/rebuild
- WAL v1: full page-image records, CRC, monotonic LSN, group-commit (fsync coalescing)
- CDC:
    - wal-tail — JSONL feed of frames
    - wal-ship — binary stream (handles mid-stream header/rotate)
    - wal-apply — idempotent apply (LSN-gated updates)
- Directory: per-bucket heads; atomic updates (tmp+rename) + header CRC
- Overflow pages: chains for large values; orphan-collection tools
- Free-list: reuse of freed pages
- Read-only readers (`Db::open_ro`): shared lock, no replay, no meta mutations
- Integrity: `check/repair` (with `--strict`), metrics

## Build

- CLI (Release):
```
  cargo build --release
```
  Binary path: `target/release/quiverdb`.

- Library: add as a dependency and use the Rust API (`Db`, `Pager`, `Directory`).

## Quick start (CLI)

- Initialize database:
```
  quiverdb init --path ./db --page-size 4096
  quiverdb dbinit --path ./db --page-size 4096 --buckets 128
```
- Put/get/delete:
```
  quiverdb dbput --path ./db --key hello --value world
  quiverdb dbget --path ./db --key hello
  quiverdb dbdel --path ./db --key hello
```
- Check/repair/metrics:
```
  quiverdb check --path ./db --strict
  quiverdb repair --path ./db
  quiverdb metrics
  quiverdb metrics-reset
```
- CDC:
```
  quiverdb wal-tail --path ./db --follow
  quiverdb wal-ship --path ./db --follow | quiverdb wal-apply --path ./follower
```
## Quick start (Rust API)
``` rust
    use anyhow::Result;
    use QuiverDB::{Db, Directory, init_db};

    fn main() -> Result<()> {
        let root = std::path::Path::new("./db");
        if !root.exists() {
            init_db(root, 4096)?;
            Directory::create(root, 128)?;
        }

        // writer (exclusive lock)
        {
            let mut db = Db::open(root)?;
            db.put(b"alpha", b"1")?;
            let v = db.get(b"alpha")?.unwrap();
            assert_eq!(v, b"1");
        }

        // read-only (shared lock)
        {
            let db_ro = Db::open_ro(root)?;
            let v = db_ro.get(b"alpha")?.unwrap();
            assert_eq!(v, b"1");
        }
        Ok(())
    }
```
## CLI cheat sheet

- Low-level: `init`, `status`, `alloc`, `read`, `write`
- High-level DB: `dbinit`, `dbput`, `dbget`, `dbdel`, `dbstats`
- Integrity/metrics: `check [--strict]`, `repair`, `metrics`, `metrics-reset`
- CDC: `wal-tail`, `wal-ship`, `wal-apply`
- Free-list: `free-status`, `free-pop`, `free-push`
- Page v2 (RH): `pagefmt-rh`, `rh-put`, `rh-get`, `rh-list`, `rh-del`, `rh-compact`

Run `quiverdb --help` for the full list and options.

## Environment variables

- `P1_WAL_COALESCE_MS` (default 3) — fsync coalescing for WAL group-commit
- `P1_DATA_FSYNC=0|1` (default 1) — fsync data segments on each commit
    - When `0`: durability relies on WAL; WAL is not truncated on commit (it will truncate during replay)
- `P1_PAGE_CACHE_PAGES=N` — page cache size (0 disables)
- `P1_OVF_THRESHOLD_BYTES=N` — value size threshold for overflow (defaults to `page_size/4`)
- `P1_RH_COMPACT_DEAD_FRAC`, `P1_RH_COMPACT_MIN_BYTES` — RH auto-compaction heuristics
- `P1_DB_GET_OUT=path` — raw output file path for `dbget`

## Crash safety and replay

- All changes go through WAL (CRC + monotonic LSN)
- Replay:
    - tolerant to partial tails (short reads = normal EOF)
    - ignores unknown record types (forward-compatible)
    - LSN-gated for page v2 (apply only when `wal_lsn > page_lsn`)
- `clean_shutdown=true` → fast start (truncate WAL to header)
- `clean_shutdown=false` → writer performs replay and truncates WAL after success

## Read-only readers

- `Db::open_ro`: shared-lock, no replay, no `clean_shutdown` changes
- Multiple readers may coexist; an exclusive writer blocks new readers while running

## Integrity: check and repair

- `quiverdb check --path ./db` — scans v2 pages, reports CRC/IO issues and overflow orphans
- `--strict` — returns a non-zero exit code when problems are detected (CI-friendly)
- `quiverdb repair` — frees overflow pages that are unreachable from directory heads and not present in the free-list (conservative)

## On-disk formats (frozen in 1.0.0)

- meta v3:
  `[MAGIC8][ver u32][page_size u32][flags u32][next_page_id u64][hash_kind u32][last_lsn u64][clean_shutdown u8]`
- page v2 header (64 B):
  `MAGIC4, ver u16=2, type u16, pad4, page_id u64, data_start u16, table_slots u16, used_slots u16, flags u16, next_page_id u64, lsn u64, seed u64, crc32 u32`
- WAL v1:
    - header (16 B): `WAL_MAGIC8 + reserved u32 + reserved u32`
    - record (28 B): `[type u8][flags u8][reserved u16][lsn u64][page_id u64][len u32][crc32 u32]`
    - payload: full page image
- Overflow placeholder in RH values (18 B):
  `[0xFF][0x00][total_len u64][head_pid u64]`

Note (Windows): directory fsync after `rename` is best-effort (on Unix we `sync_all()` the parent directory).

## Roadmap (post‑1.0)

- v1.1: CDC Outbox (stdout/TCP/Kafka sinks), live subscriptions on WAL‑tail, deterministic replay tooling
- v1.2: Snapshot Isolation (COW+LSN) and incremental backup with deduplication

## License

MIT — see LICENSE.