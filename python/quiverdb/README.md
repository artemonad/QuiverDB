# quiverdb (Python)

Python bindings for QuiverDB (embedded KV DB with Robin Hood index, WAL, snapshots, backup/restore).  
Backed by Rust via PyO3 and maturin.

Status: v1.2.5 — on-disk formats remain frozen (meta v3, page v2, WAL v1).  
Phase 1: in-process snapshots + backup/restore.  
Phase 2: persisted snapshots (registry) + SnapStore (content-addressed dedup).

## Install (local)

Prerequisites:
- Rust toolchain (stable)
- Python 3.8+
- maturin

```bash
pip install maturin
maturin develop -m python/quiverdb/pyproject.toml
```

This builds the extension module in-place and installs it into your active virtual environment.

## Quick start

```python
from quiverdb import Database

# Initialize DB and directory if missing
Database.init_db("./db_py", 4096, 128)

# Open writer with config overrides (kwargs)
db = Database.open(
    "./db_py",
    wal_coalesce_ms=0,
    data_fsync=True,
    page_cache_pages=8,
    # Phase 2:
    snap_persist=True,    # keep sidecar + track snapshot registry
    snap_dedup=True,      # enable SnapStore (dedup + hashindex)
    snapstore_dir="./.snapstore_alt"  # optional custom SnapStore directory
)

# Basic KV operations
db.put(b"alpha", b"1")
assert db.get(b"alpha") == b"1"

# Scan (tail-wins semantics)
print(db.scan_prefix(b"a"))

# One-shot backup (Phase 1)
db.backup("./backup_py")

# Restore to another path
Database.restore("./db_restored_py", "./backup_py")
```

## API (Python)

Class: quiverdb.Database

- static init_db(path: str, page_size: int, buckets: int) -> None  
  Initialize a new DB if missing (meta/seg1/WAL/free + directory).

- static open(path: str, ..., wal_coalesce_ms=None, data_fsync=None, page_cache_pages=None, ovf_threshold_bytes=None, snap_persist=None, snap_dedup=None, snapstore_dir=None) -> Database  
  Open a writer handle (exclusive lock) with optional config overrides.  
  Notes:
  - Unspecified options are taken from environment via QuiverConfig::from_env().
  - Phase 2 options:
    - snap_persist: bool — keep sidecar and write .snapshots/registry.json
    - snap_dedup: bool — enable SnapStore dedup + sidecar hashindex.bin
    - snapstore_dir: str — custom directory for SnapStore (absolute or relative to DB root)

- static open_ro(path: str) -> Database  
  Open a read-only handle (shared lock). Respects Phase 2 env flags (if set).

- put(key: bytes, value: bytes) -> None

- get(key: bytes) -> Optional[bytes]

- delete(key: bytes) -> bool

- scan_all() -> list[tuple[bytes, bytes]]

- scan_prefix(prefix: bytes) -> list[tuple[bytes, bytes]]

- backup(out_dir: str, since_lsn: Optional[int] = None) -> None  
  One-shot snapshot + backup (full if since_lsn=None, otherwise incremental).

- static restore(dst_path: str, backup_dir: str) -> None

- last_lsn() -> int  
  Convenience helper.

## Environment variables (honored by defaults)

- P1_WAL_COALESCE_MS — WAL fsync coalescing (ms)
- P1_DATA_FSYNC=0|1 — fsync data segments per commit
- P1_PAGE_CACHE_PAGES — page cache size
- P1_OVF_THRESHOLD_BYTES — overflow threshold (default page_size/4)
- Phase 2:
  - P1_SNAP_PERSIST=0|1 — persisted snapshots (keep sidecar + registry)
  - P1_SNAP_DEDUP=0|1 — enable SnapStore dedup and sidecar hashindex
  - P1_SNAPSTORE_DIR=path — custom SnapStore directory (absolute or relative to DB root)

Note: When using Database.open(...), explicit kwargs override env-derived defaults.

## Notes

- Single-writer, multi-reader is enforced via file locks.
- Crash safety via WAL (full page images, CRC, LSN-gating). WAL replay is executed by a writer on unclean shutdown.
- Phase 1 snapshots are in-process; Phase 2 enables persisted snapshots with dedup (SnapStore).
- On-disk formats are frozen across 1.x (no migrations required).

## License

MIT — see the root LICENSE.