use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};

use quiverdb_core as qdb;
use qdb::Directory;

fn pyerr<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

// Db внутри содержит несендобельные поля (RefCell и т.п.) — помечаем как unsendable.
#[pyclass(unsendable)]
struct Database {
    inner: qdb::Db,
}

#[pymethods]
impl Database {
    /// Create a new DB if missing: meta/seg1/WAL/free + directory.
    #[staticmethod]
    pub fn init_db(path: &str, page_size: u32, buckets: u32) -> PyResult<()> {
        qdb::init_db(std::path::Path::new(path), page_size).map_err(pyerr)?;
        Directory::create(std::path::Path::new(path), buckets).map_err(pyerr)?;
        Ok(())
    }

    /// Open writer with optional config overrides (kwargs).
    /// Example:
    ///   Database.open("./db",
    ///                 wal_coalesce_ms=0,
    ///                 data_fsync=True,
    ///                 page_cache_pages=256,
    ///                 ovf_threshold_bytes=None,
    ///                 snap_persist=True,
    ///                 snap_dedup=True,
    ///                 snapstore_dir="./.snapstore_alt")
    #[staticmethod]
    #[pyo3(signature = (
        path,
        wal_coalesce_ms=None,
        data_fsync=None,
        page_cache_pages=None,
        ovf_threshold_bytes=None,
        snap_persist=None,
        snap_dedup=None,
        snapstore_dir=None
    ))]
    pub fn open(
        path: &str,
        wal_coalesce_ms: Option<u64>,
        data_fsync: Option<bool>,
        page_cache_pages: Option<usize>,
        ovf_threshold_bytes: Option<usize>,
        snap_persist: Option<bool>,
        snap_dedup: Option<bool>,
        snapstore_dir: Option<String>,
    ) -> PyResult<Self> {
        let mut cfg = qdb::config::QuiverConfig::from_env();
        if let Some(v) = wal_coalesce_ms { cfg.wal_coalesce_ms = v; }
        if let Some(v) = data_fsync { cfg.data_fsync = v; }
        if let Some(v) = page_cache_pages { cfg.page_cache_pages = v; }
        if let Some(v) = ovf_threshold_bytes { cfg.ovf_threshold_bytes = Some(v); }
        if let Some(v) = snap_persist { cfg.snap_persist = v; }
        if let Some(v) = snap_dedup { cfg.snap_dedup = v; }
        if let Some(v) = snapstore_dir {
            if !v.trim().is_empty() {
                cfg.snapstore_dir = Some(v);
            }
        }

        let db = qdb::Db::open_with_config(std::path::Path::new(path), cfg).map_err(pyerr)?;
        Ok(Self { inner: db })
    }

    /// Open read-only (shared lock). Uses env-derived config (Phase 2 env flags are respected).
    #[staticmethod]
    pub fn open_ro(path: &str) -> PyResult<Self> {
        let cfg = qdb::config::QuiverConfig::from_env();
        let db = qdb::Db::open_ro_with_config(std::path::Path::new(path), cfg).map_err(pyerr)?;
        Ok(Self { inner: db })
    }

    /// Put key/value (bytes).
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> PyResult<()> {
        self.inner.put(key, value).map_err(pyerr)
    }

    /// Get key -> Optional[bytes].
    pub fn get<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let v = self.inner.get(key).map_err(pyerr)?;
        Ok(v.map(|b| PyBytes::new(py, &b)))
    }

    /// Delete key -> bool (True if existed).
    pub fn delete(&mut self, key: &[u8]) -> PyResult<bool> {
        self.inner.del(key).map_err(pyerr)
    }

    /// Scan all -> List[(bytes, bytes)].
    pub fn scan_all<'py>(&self, py: Python<'py>) -> PyResult<Vec<(Bound<'py, PyBytes>, Bound<'py, PyBytes>)>> {
        let pairs = self.inner.scan_all().map_err(pyerr)?;
        Ok(pairs
            .into_iter()
            .map(|(k, v)| (PyBytes::new(py, &k), PyBytes::new(py, &v)))
            .collect())
    }

    /// Scan by prefix -> List[(bytes, bytes)].
    pub fn scan_prefix<'py>(&self, py: Python<'py>, prefix: &[u8]) -> PyResult<Vec<(Bound<'py, PyBytes>, Bound<'py, PyBytes>)>> {
        let pairs = self.inner.scan_prefix(prefix).map_err(pyerr)?;
        Ok(pairs
            .into_iter()
            .map(|(k, v)| (PyBytes::new(py, &k), PyBytes::new(py, &v)))
            .collect())
    }

    /// One-shot snapshot + backup (Phase 1).
    #[pyo3(signature = (out_dir, since_lsn=None))]
    pub fn backup(&mut self, out_dir: &str, since_lsn: Option<u64>) -> PyResult<()> {
        let mut snap = self.inner.snapshot_begin().map_err(pyerr)?;
        qdb::backup::backup_to_dir(&self.inner, &snap, std::path::Path::new(out_dir), since_lsn)
            .map_err(pyerr)?;
        self.inner.snapshot_end(&mut snap).map_err(pyerr)?;
        Ok(())
    }

    /// Restore from backup dir into destination DB path.
    #[staticmethod]
    pub fn restore(dst_path: &str, backup_dir: &str) -> PyResult<()> {
        qdb::backup::restore_from_dir(
            std::path::Path::new(dst_path),
            std::path::Path::new(backup_dir),
        )
            .map_err(pyerr)
    }

    /// Return last LSN (helper).
    pub fn last_lsn(&self) -> PyResult<u64> {
        Ok(self.inner.pager.meta.last_lsn)
    }
}

/// pyo3 0.26: модуль принимает &Bound<PyModule>.
#[pymodule]
fn quiverdb(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add("version", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}