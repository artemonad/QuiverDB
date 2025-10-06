# quiverdb (Python)

Python bindings for QuiverDB via PyO3.

## Install (local)
- Ensure Rust toolchain is installed
- Install maturin: `pip install maturin`
- Build and develop (editable) from the project root:
  `maturin develop -m python/quiverdb/pyproject.toml`

## Usage
```python
from quiverdb import Database

# Init if missing
Database.init_db("./db_py", 4096, 128)

# Open
db = Database.open("./db_py", wal_coalesce_ms=0, data_fsync=True, page_cache_pages=8)

# Put/Get
db.put(b"alpha", b"1")
assert db.get(b"alpha") == b"1"

# Scan
print(db.scan_prefix(b"a"))

# Snapshot backup (Phase 1)
db.backup("./backup_py")

# Restore to another path
Database.restore("./db_restored_py", "./backup_py")