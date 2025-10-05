//! Lightweight global metrics for QuiverDB.
//!
//! Потокобезопасные атомарные счётчики для подсистем:
//! - WAL
//! - Page cache
//! - Robin Hood compaction
//! - Overflow (v0.6)
//! - Sweep orphan (v0.6)
//! - Snapshots / Backup / Restore (v1.2 scaffold)

use std::sync::atomic::{AtomicU64, Ordering};

// ----- WAL -----
static WAL_APPENDS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WAL_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
static WAL_FSYNC_CALLS: AtomicU64 = AtomicU64::new(0);
static WAL_FSYNC_BATCH_PAGES: AtomicU64 = AtomicU64::new(0);
static WAL_TRUNCATIONS: AtomicU64 = AtomicU64::new(0);

// ----- Page cache -----
static PAGE_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static PAGE_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

// ----- Robin Hood -----
static RH_PAGE_COMPACTIONS: AtomicU64 = AtomicU64::new(0);

// ----- Overflow (v0.6) -----
static OVF_CHAINS_CREATED: AtomicU64 = AtomicU64::new(0);
static OVF_CHAINS_FREED: AtomicU64 = AtomicU64::new(0);

// ----- Sweep orphan (v0.6) -----
static SWEEP_ORPHAN_RUNS: AtomicU64 = AtomicU64::new(0);

// ----- Snapshots / Backup / Restore (v1.2 scaffold) -----
static SNAPSHOTS_ACTIVE: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_FREEZE_FRAMES: AtomicU64 = AtomicU64::new(0);
static SNAPSHOT_FREEZE_BYTES: AtomicU64 = AtomicU64::new(0);
static BACKUP_PAGES_EMITTED: AtomicU64 = AtomicU64::new(0);
static BACKUP_BYTES_EMITTED: AtomicU64 = AtomicU64::new(0);
static RESTORE_PAGES_WRITTEN: AtomicU64 = AtomicU64::new(0);
static RESTORE_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    // WAL
    pub wal_appends_total: u64,
    pub wal_bytes_written: u64,
    pub wal_fsync_calls: u64,
    pub wal_fsync_batch_pages: u64,
    pub wal_truncations: u64,

    // Page cache
    pub page_cache_hits: u64,
    pub page_cache_misses: u64,

    // Robin Hood
    pub rh_page_compactions: u64,

    // Overflow (v0.6)
    pub overflow_chains_created: u64,
    pub overflow_chains_freed: u64,

    // Sweep orphan (v0.6)
    pub sweep_orphan_runs: u64,

    // Snapshots / Backup / Restore (v1.2 scaffold)
    pub snapshots_active: u64,
    pub snapshot_freeze_frames: u64,
    pub snapshot_freeze_bytes: u64,
    pub backup_pages_emitted: u64,
    pub backup_bytes_emitted: u64,
    pub restore_pages_written: u64,
    pub restore_bytes_written: u64,
}

impl MetricsSnapshot {
    pub fn avg_wal_batch_pages(&self) -> f64 {
        if self.wal_fsync_calls == 0 {
            0.0
        } else {
            self.wal_fsync_batch_pages as f64 / self.wal_fsync_calls as f64
        }
    }

    pub fn cache_hit_ratio(&self) -> f64 {
        let total = self.page_cache_hits + self.page_cache_misses;
        if total == 0 {
            0.0
        } else {
            self.page_cache_hits as f64 / total as f64
        }
    }
}

// ----- Recorders (WAL) -----
pub fn record_wal_append(payload_len: usize) {
    WAL_APPENDS_TOTAL.fetch_add(1, Ordering::Relaxed);
    WAL_BYTES_WRITTEN.fetch_add(payload_len as u64, Ordering::Relaxed);
}

pub fn record_wal_fsync(batch_pages: u64) {
    WAL_FSYNC_CALLS.fetch_add(1, Ordering::Relaxed);
    WAL_FSYNC_BATCH_PAGES.fetch_add(batch_pages, Ordering::Relaxed);
}

pub fn record_wal_truncation() {
    WAL_TRUNCATIONS.fetch_add(1, Ordering::Relaxed);
}

// ----- Recorders (Page cache) -----
pub fn record_cache_hit() {
    PAGE_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
}
pub fn record_cache_miss() {
    PAGE_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
}

// ----- Recorders (Robin Hood) -----
pub fn record_rh_compaction() {
    RH_PAGE_COMPACTIONS.fetch_add(1, Ordering::Relaxed);
}

// ----- Recorders (Overflow) -----
pub fn record_overflow_chain_created() {
    OVF_CHAINS_CREATED.fetch_add(1, Ordering::Relaxed);
}
pub fn record_overflow_chain_freed() {
    OVF_CHAINS_FREED.fetch_add(1, Ordering::Relaxed);
}

// ----- Recorders (Sweep orphan) -----
pub fn record_sweep_orphan_run() {
    SWEEP_ORPHAN_RUNS.fetch_add(1, Ordering::Relaxed);
}

// ----- Recorders (Snapshots / Backup / Restore) -----
pub fn record_snapshot_begin() {
    SNAPSHOTS_ACTIVE.fetch_add(1, Ordering::Relaxed);
}

pub fn record_snapshot_end() {
    SNAPSHOTS_ACTIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1))).ok();
}

pub fn record_snapshot_freeze_frame(bytes: usize) {
    SNAPSHOT_FREEZE_FRAMES.fetch_add(1, Ordering::Relaxed);
    SNAPSHOT_FREEZE_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub fn record_backup_page_emitted(bytes: usize) {
    BACKUP_PAGES_EMITTED.fetch_add(1, Ordering::Relaxed);
    BACKUP_BYTES_EMITTED.fetch_add(bytes as u64, Ordering::Relaxed);
}

pub fn record_restore_page_written(bytes: usize) {
    RESTORE_PAGES_WRITTEN.fetch_add(1, Ordering::Relaxed);
    RESTORE_BYTES_WRITTEN.fetch_add(bytes as u64, Ordering::Relaxed);
}

// ----- Snapshot / Reset -----
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

        overflow_chains_created: OVF_CHAINS_CREATED.load(Ordering::Relaxed),
        overflow_chains_freed: OVF_CHAINS_FREED.load(Ordering::Relaxed),

        sweep_orphan_runs: SWEEP_ORPHAN_RUNS.load(Ordering::Relaxed),

        snapshots_active: SNAPSHOTS_ACTIVE.load(Ordering::Relaxed),
        snapshot_freeze_frames: SNAPSHOT_FREEZE_FRAMES.load(Ordering::Relaxed),
        snapshot_freeze_bytes: SNAPSHOT_FREEZE_BYTES.load(Ordering::Relaxed),
        backup_pages_emitted: BACKUP_PAGES_EMITTED.load(Ordering::Relaxed),
        backup_bytes_emitted: BACKUP_BYTES_EMITTED.load(Ordering::Relaxed),
        restore_pages_written: RESTORE_PAGES_WRITTEN.load(Ordering::Relaxed),
        restore_bytes_written: RESTORE_BYTES_WRITTEN.load(Ordering::Relaxed),
    }
}

pub fn reset() {
    WAL_APPENDS_TOTAL.store(0, Ordering::Relaxed);
    WAL_BYTES_WRITTEN.store(0, Ordering::Relaxed);
    WAL_FSYNC_CALLS.store(0, Ordering::Relaxed);
    WAL_FSYNC_BATCH_PAGES.store(0, Ordering::Relaxed);
    WAL_TRUNCATIONS.store(0, Ordering::Relaxed);

    PAGE_CACHE_HITS.store(0, Ordering::Relaxed);
    PAGE_CACHE_MISSES.store(0, Ordering::Relaxed);

    RH_PAGE_COMPACTIONS.store(0, Ordering::Relaxed);

    OVF_CHAINS_CREATED.store(0, Ordering::Relaxed);
    OVF_CHAINS_FREED.store(0, Ordering::Relaxed);

    SWEEP_ORPHAN_RUNS.store(0, Ordering::Relaxed);

    SNAPSHOTS_ACTIVE.store(0, Ordering::Relaxed);
    SNAPSHOT_FREEZE_FRAMES.store(0, Ordering::Relaxed);
    SNAPSHOT_FREEZE_BYTES.store(0, Ordering::Relaxed);
    BACKUP_PAGES_EMITTED.store(0, Ordering::Relaxed);
    BACKUP_BYTES_EMITTED.store(0, Ordering::Relaxed);
    RESTORE_PAGES_WRITTEN.store(0, Ordering::Relaxed);
    RESTORE_BYTES_WRITTEN.store(0, Ordering::Relaxed);
}