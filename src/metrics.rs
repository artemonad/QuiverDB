//! Lightweight global metrics for p1.
//! Usage: call record_* from subsystems, and snapshot() to read in CLI/DbStats.

use std::sync::atomic::{AtomicU64, Ordering};

static WAL_APPENDS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WAL_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
static WAL_FSYNC_CALLS: AtomicU64 = AtomicU64::new(0);
static WAL_FSYNC_BATCH_PAGES: AtomicU64 = AtomicU64::new(0);
static WAL_TRUNCATIONS: AtomicU64 = AtomicU64::new(0);

static PAGE_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static PAGE_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

static RH_PAGE_COMPACTIONS: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    pub wal_appends_total: u64,
    pub wal_bytes_written: u64,
    pub wal_fsync_calls: u64,
    pub wal_fsync_batch_pages: u64,
    pub wal_truncations: u64,

    pub page_cache_hits: u64,
    pub page_cache_misses: u64,

    pub rh_page_compactions: u64,
}

impl MetricsSnapshot {
    pub fn avg_wal_batch_pages(&self) -> f64 {
        if self.wal_fsync_calls == 0 {
            0.0
        } else {
            self.wal_fsync_batch_pages as f64 / self.wal_fsync_calls as f64
        }
    }
}

/// Record one WAL append of a page record.
pub fn record_wal_append(payload_len: usize) {
    WAL_APPENDS_TOTAL.fetch_add(1, Ordering::Relaxed);
    WAL_BYTES_WRITTEN.fetch_add(payload_len as u64, Ordering::Relaxed);
}

/// Record a WAL fsync completion. batch_pages = how many page records were made durable by this fsync.
pub fn record_wal_fsync(batch_pages: u64) {
    WAL_FSYNC_CALLS.fetch_add(1, Ordering::Relaxed);
    WAL_FSYNC_BATCH_PAGES.fetch_add(batch_pages, Ordering::Relaxed);
}

/// Record WAL truncation (rotation to header).
pub fn record_wal_truncation() {
    WAL_TRUNCATIONS.fetch_add(1, Ordering::Relaxed);
}

/// Record page cache hit/miss.
pub fn record_cache_hit() {
    PAGE_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
}
pub fn record_cache_miss() {
    PAGE_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
}

/// Record a Robin Hood page compaction.
pub fn record_rh_compaction() {
    RH_PAGE_COMPACTIONS.fetch_add(1, Ordering::Relaxed);
}

/// Read current metrics atomically (not clearing them).
pub fn snapshot() -> MetricsSnapshot {
    MetricsSnapshot {
        wal_appends_total: WAL_APPENDS_TOTAL.load(Ordering::Relaxed),
        wal_bytes_written: WAL_BYTES_WRITTEN.load(Ordering::Relaxed),
        wal_fsync_calls: WAL_FSYNC_CALLS.load(Ordering::Relaxed),
        wal_fsync_batch_pages: WAL_FSYNC_BATCH_PAGES.load(Ordering::Relaxed),
        wal_truncations: WAL_TRUNCATIONS.load(Ordering::Relaxed),

        page_cache_hits: PAGE_CACHE_HITS.load(Ordering::Relaxed),
        page_cache_misses: PAGE_CACHE_MISSES.load(Ordering::Relaxed),

        rh_page_compactions: RH_PAGE_COMPACTIONS.load(Ordering::Relaxed),
    }
}

/// Reset all metrics (useful in tests/bench).
pub fn reset() {
    WAL_APPENDS_TOTAL.store(0, Ordering::Relaxed);
    WAL_BYTES_WRITTEN.store(0, Ordering::Relaxed);
    WAL_FSYNC_CALLS.store(0, Ordering::Relaxed);
    WAL_FSYNC_BATCH_PAGES.store(0, Ordering::Relaxed);
    WAL_TRUNCATIONS.store(0, Ordering::Relaxed);

    PAGE_CACHE_HITS.store(0, Ordering::Relaxed);
    PAGE_CACHE_MISSES.store(0, Ordering::Relaxed);

    RH_PAGE_COMPACTIONS.store(0, Ordering::Relaxed);
}