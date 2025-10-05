//! Centralized configuration and builder for QuiverDB.
//!
//! Goals:
//! - Single place to collect tunables instead of scattering env lookups.
//! - Keep backward compatibility: QuiverConfig::from_env() reads the same env vars.
//! - Provide a simple DbBuilder that returns a QuiverConfig, which Db will consume.
//
//! Usage (next step will wire this into Db):
//!   let cfg = QuiverConfig::from_env()
//!       .with_wal_coalesce_ms(5)
//!       .with_data_fsync(false)
//!       .with_page_cache_pages(256);
//!
//!   // Db::open_with_config(path, cfg)  <-- will be added in Db next step.

use std::fmt;

/// Top-level configuration for QuiverDB (writer/reader).
/// Backward-compatible with env-based configuration used so far.
#[derive(Clone, Debug)]
pub struct QuiverConfig {
    /// WAL fsync coalescing window in milliseconds (group-commit).
    /// Env: P1_WAL_COALESCE_MS (default 3)
    pub wal_coalesce_ms: u64,

    /// Whether to fsync data segments on every commit (besides durable WAL).
    /// Env: P1_DATA_FSYNC (default true; "0|false|off|no" => false)
    pub data_fsync: bool,

    /// Page cache size in pages (0 disables).
    /// Env: P1_PAGE_CACHE_PAGES (default 0)
    pub page_cache_pages: usize,

    /// Optional explicit overflow threshold in bytes; if None, defaults to page_size/4.
    /// Env: P1_OVF_THRESHOLD_BYTES (default None, meaning "use ps/4").
    pub ovf_threshold_bytes: Option<usize>,
}

impl Default for QuiverConfig {
    fn default() -> Self {
        Self {
            wal_coalesce_ms: 3,
            data_fsync: true,
            page_cache_pages: 0,
            ovf_threshold_bytes: None,
        }
    }
}

impl QuiverConfig {
    /// Load configuration from environment variables (keeps backward-compatible behavior).
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        if let Ok(v) = std::env::var("P1_WAL_COALESCE_MS") {
            if let Ok(n) = v.trim().parse::<u64>() {
                cfg.wal_coalesce_ms = n;
            }
        }

        if let Ok(v) = std::env::var("P1_DATA_FSYNC") {
            let s = v.trim().to_ascii_lowercase();
            cfg.data_fsync = !(s == "0" || s == "false" || s == "off" || s == "no");
        }

        if let Ok(v) = std::env::var("P1_PAGE_CACHE_PAGES") {
            if let Ok(n) = v.trim().parse::<usize>() {
                cfg.page_cache_pages = n;
            }
        }

        if let Ok(v) = std::env::var("P1_OVF_THRESHOLD_BYTES") {
            if let Ok(n) = v.trim().parse::<usize>() {
                cfg.ovf_threshold_bytes = Some(n);
            }
        }

        cfg
    }

    /// Fluent setters (builder-style) to override specific fields.

    pub fn with_wal_coalesce_ms(mut self, ms: u64) -> Self {
        self.wal_coalesce_ms = ms;
        self
    }

    pub fn with_data_fsync(mut self, on: bool) -> Self {
        self.data_fsync = on;
        self
    }

    pub fn with_page_cache_pages(mut self, pages: usize) -> Self {
        self.page_cache_pages = pages;
        self
    }

    pub fn with_ovf_threshold_bytes(mut self, thr: Option<usize>) -> Self {
        self.ovf_threshold_bytes = thr;
        self
    }
}

/// Lightweight builder that produces a QuiverConfig.
/// Db will expose `Db::builder()` returning this builder in the next step.
#[derive(Clone, Debug)]
pub struct DbBuilder {
    cfg: QuiverConfig,
}

impl Default for DbBuilder {
    fn default() -> Self {
        // Start from env to preserve current behavior, then allow overrides.
        Self { cfg: QuiverConfig::from_env() }
    }
}

impl DbBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start from a clean default (without reading env).
    pub fn from_default() -> Self {
        Self { cfg: QuiverConfig::default() }
    }

    pub fn wal_coalesce_ms(mut self, ms: u64) -> Self {
        self.cfg.wal_coalesce_ms = ms;
        self
    }

    pub fn data_fsync(mut self, on: bool) -> Self {
        self.cfg.data_fsync = on;
        self
    }

    pub fn page_cache_pages(mut self, pages: usize) -> Self {
        self.cfg.page_cache_pages = pages;
        self
    }

    pub fn ovf_threshold_bytes(mut self, thr: Option<usize>) -> Self {
        self.cfg.ovf_threshold_bytes = thr;
        self
    }

    /// Finish the builder and obtain the configuration.
    pub fn build(self) -> QuiverConfig {
        self.cfg
    }
}

impl fmt::Display for QuiverConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "QuiverConfig {{ wal_coalesce_ms: {}, data_fsync: {}, page_cache_pages: {}, ovf_threshold_bytes: {} }}",
            self.wal_coalesce_ms,
            self.data_fsync,
            self.page_cache_pages,
            self.ovf_threshold_bytes
                .map(|v| v.to_string())
                .unwrap_or_else(|| "default(ps/4)".to_string())
        )
    }
}