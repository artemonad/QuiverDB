use anyhow::Result;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use quiverdb_core as core;
use core::{Directory};

fn pyerr<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

#[pyclass]
struct Database {
    inner: core::Db,
}

#[pymethods]
impl Database {
    /// Create a new DB if missing: meta/seg1/WAL/free + directory.
    #[staticmethod]
    pub fn init_db(path: &str, page_size: u32, buckets: u32) -> PyResult<()> {
        core::init_db(std::path::Path::new(path), page_size).map_err(pyerr)?;
        Directory::create(std::path::Path::new(path), buckets).map_err(pyerr)?;
        Ok(())
    }

    /// Open writer with optional config overrides (kwargs).
    /// Example: Database.open("./db", wal_coalesce_ms=0, data_fsync=True, page_cache_pages=256)
    #[staticmethod]
    #[pyo3(signature = (path, wal_coalesce_ms=None, data_fsync=None, page_cache_pages=None, ovf_threshold_bytes=None))]
    pub fn open(
        path: &str,
        wal_coalesce_ms: Option<u64>,
        data_fsync: Option<bool>,
        page_cache_pages: Option<usize>,
        ovf_threshold_bytes: Option<usize>,
    ) -> PyResult<Self> {
        let mut cfg = core::config::QuiverConfig::from_env();
        if let Some(v) = wal_coalesce_ms { cfg.wal_coalesce_ms = v; }
        if let Some(v) = data_fsync { cfg.data_fsync = v; }
        if let Some(v) = page_cache_pages { cfg.page_cache_pages = v; }
        if let Some(v) = ovf_threshold_bytes { cfg.ovf_threshold_bytes = Some(v); }

        let db = core::Db::open_with_config(std::path::Path::new(path), cfg).map_err(pyerr)?;
        Ok(Self { inner: db })
    }

    /// Open read-only.
    #[staticmethod]
    pub fn open_ro(path: &str) -> PyResult<Self> {
        let cfg = core::config::QuiverConfig::from_env();
        let db = core::Db::open_ro_with_config(std::path::Path::new(path), cfg).map_err(pyerr)?;
        Ok(Self { inner: db })
    }

    /// Put key/value (bytes).
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> PyResult<()> {
        self.inner.put(key, value).map_err(pyerr)
    }

    /// Get key -> Optional[bytes].
    pub fn get<'py>(&self, py: Python<'py>, key: &[u8]) -> PyResult<Option<&'py PyBytes>> {
        let v = self.inner.get(key).map_err(pyerr)?;
        Ok(v.map(|b| PyBytes::new(py, &b)))
    }

    /// Delete key -> bool (True if existed).
    pub fn delete(&mut self, key: &[u8]) -> PyResult<bool> {
        self.inner.del(key).map_err(pyerr)
    }

    /// Scan all -> List[(bytes, bytes)] (simple materialization).
    pub fn scan_all<'py>(&self, py: Python<'py>) -> PyResult<Vec<(&'py PyBytes, &'py PyBytes)>> {
        let pairs = self.inner.scan_all().map_err(pyerr)?;
        Ok(pairs.into_iter()
            .map(|(k, v)| (PyBytes::new(py, &k), PyBytes::new(py, &v)))
            .collect())
    }

    /// Scan by prefix -> List[(bytes, bytes)].
    pub fn scan_prefix<'py>(&self, py: Python<'py>, prefix: &[u8]) -> PyResult<Vec<(&'py PyBytes, &'py PyBytes)>> {
        let pairs = self.inner.scan_prefix(prefix).map_err(pyerr)?;
        Ok(pairs.into_iter()
            .map(|(k, v)| (PyBytes::new(py, &k), PyBytes::new(py, &v)))
            .collect())
    }

    /// One-shot snapshot + backup (Phase 1).
    #[pyo3(signature = (out_dir, since_lsn=None))]
    pub fn backup(&mut self, out_dir: &str, since_lsn: Option<u64>) -> PyResult<()> {
        use core::backup::{backup_to_dir};
        let mut snap = self.inner.snapshot_begin().map_err(pyerr)?;
        backup_to_dir(&self.inner, &snap, std::path::Path::new(out_dir), since_lsn).map_err(pyerr)?;
        self.inner.snapshot_end(&mut snap).map_err(pyerr)?;
        Ok(())
    }

    /// Restore from backup dir into destination DB path.
    #[staticmethod]
    pub fn restore(dst_path: &str, backup_dir: &str) -> PyResult<()> {
        use core::backup::restore_from_dir;
        restore_from_dir(std::path::Path::new(dst_path), std::path::Path::new(backup_dir)).map_err(pyerr)
    }

    /// Return last LSN (helper).
    pub fn last_lsn(&self) -> PyResult<u64> {
        Ok(self.inner.pager.meta.last_lsn)
    }
}

#[pymodule]
fn quiverdb(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Database>()?;
    m.add("version", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}