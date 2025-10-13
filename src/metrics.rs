//! Lightweight global metrics for QuiverDB.
//!
//! Потокобезопасные атомарные счётчики для подсистем:
//! - WAL
//! - Page cache
//! - In‑memory keydir fast‑path
//! - Robin Hood compaction
//! - Overflow (v0.6)
//! - Sweep orphan (v0.6)
//! - Snapshots / Backup / Restore (v1.2 scaffold)
//! - TTL (read-side)
//! - Bloom (fast-path)
//! - WAL LSN gauges — pending_max_lsn / flushed_lsn
//! - Packing — учёт упакованных KV‑страниц и записей
//! - Bloom delta-update — учёт обновлений bloom.bin из batch
//! - NEW: Lazy compaction — срабатывания и переписанные страницы
//! - NEW: WAL threshold flush — счётчики пороговых fsync вне явного батча

use std::sync::atomic::{AtomicU64, Ordering};

// ----- WAL -----
static WAL_APPENDS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WAL_BYTES_WRITTEN: AtomicU64 = AtomicU64::new(0);
static WAL_FSYNC_CALLS: AtomicU64 = AtomicU64::new(0);
static WAL_FSYNC_BATCH_PAGES: AtomicU64 = AtomicU64::new(0);
static WAL_TRUNCATIONS: AtomicU64 = AtomicU64::new(0);

// NEW: WAL LSN gauges
static WAL_PENDING_MAX_LSN: AtomicU64 = AtomicU64::new(0);
static WAL_FLUSHED_LSN: AtomicU64 = AtomicU64::new(0);

// NEW: WAL threshold flush counters
static WAL_THRESHOLD_FLUSHES: AtomicU64 = AtomicU64::new(0);
static WAL_THRESHOLD_FLUSH_PAGES: AtomicU64 = AtomicU64::new(0);
static WAL_THRESHOLD_FLUSH_BYTES: AtomicU64 = AtomicU64::new(0);

// ----- Page cache -----
static PAGE_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static PAGE_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

// ----- In-memory keydir fast-path -----
static KEYDIR_HITS: AtomicU64 = AtomicU64::new(0);
static KEYDIR_MISSES: AtomicU64 = AtomicU64::new(0);

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

// NEW: редкая ветка — fallback-сканы под снапшотами
static SNAPSHOT_FALLBACK_SCANS: AtomicU64 = AtomicU64::new(0);

// NEW: TTL read-side — сколько раз запись была пропущена из-за истечения срока
static TTL_SKIPPED: AtomicU64 = AtomicU64::new(0);

// NEW: Bloom fast-path (v2.0.x)
static BLOOM_TESTS: AtomicU64 = AtomicU64::new(0);
static BLOOM_NEGATIVE: AtomicU64 = AtomicU64::new(0);
static BLOOM_POSITIVE: AtomicU64 = AtomicU64::new(0);
static BLOOM_SKIPPED_STALE: AtomicU64 = AtomicU64::new(0);

// NEW: Bloom delta-update (batch)
static BLOOM_UPDATES_TOTAL: AtomicU64 = AtomicU64::new(0);
static BLOOM_UPDATE_BYTES: AtomicU64 = AtomicU64::new(0);

// NEW: Packing (KV page packing)
static PACK_PAGES: AtomicU64 = AtomicU64::new(0);
static PACK_RECORDS: AtomicU64 = AtomicU64::new(0);
static PACK_PAGES_SINGLE: AtomicU64 = AtomicU64::new(0);

// NEW: Lazy compaction
static LAZY_COMPACT_RUNS: AtomicU64 = AtomicU64::new(0);
static LAZY_COMPACT_PAGES_WRITTEN: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    // WAL
    pub wal_appends_total: u64,
    pub wal_bytes_written: u64,
    pub wal_fsync_calls: u64,
    pub wal_fsync_batch_pages: u64,
    pub wal_truncations: u64,

    // NEW: WAL LSN gauges
    pub wal_pending_max_lsn: u64,
    pub wal_flushed_lsn: u64,

    // NEW: WAL threshold flush counters
    pub wal_threshold_flushes: u64,
    pub wal_threshold_flush_pages: u64,
    pub wal_threshold_flush_bytes: u64,

    // Page cache
    pub page_cache_hits: u64,
    pub page_cache_misses: u64,

    // NEW: in-memory keydir fast-path
    pub keydir_hits: u64,
    pub keydir_misses: u64,

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

    // NEW: fallback-сканы под снапшотами
    pub snapshot_fallback_scans: u64,

    // NEW: TTL read-side
    pub ttl_skipped: u64,

    // NEW: Bloom fast-path
    pub bloom_tests: u64,
    pub bloom_negative: u64,
    pub bloom_positive: u64,
    pub bloom_skipped_stale: u64,

    // NEW: Bloom delta-update
    pub bloom_updates_total: u64,
    pub bloom_update_bytes: u64,

    // NEW: Packing
    pub pack_pages: u64,
    pub pack_records: u64,
    pub pack_pages_single: u64,

    // NEW: Lazy compaction
    pub lazy_compact_runs: u64,
    pub lazy_compact_pages_written: u64,
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

    pub fn keydir_hit_ratio(&self) -> f64 {
        let total = self.keydir_hits + self.keydir_misses;
        if total == 0 {
            0.0
        } else {
            self.keydir_hits as f64 / total as f64
        }
    }

    pub fn avg_pack_records_per_page(&self) -> f64 {
        if self.pack_pages == 0 {
            0.0
        } else {
            self.pack_records as f64 / self.pack_pages as f64
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

// NEW: gauges — обновляются на каждом кадре и после fsync
pub fn record_wal_pending_lsn(lsn: u64) {
    // max(old, lsn)
    let mut cur = WAL_PENDING_MAX_LSN.load(Ordering::Relaxed);
    while lsn > cur {
        match WAL_PENDING_MAX_LSN.compare_exchange_weak(
            cur,
            lsn,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(v) => cur = v,
        }
    }
}

pub fn record_wal_flushed_lsn(lsn: u64) {
    // max(old, lsn) — на случай нестрогого порядка
    let mut cur = WAL_FLUSHED_LSN.load(Ordering::Relaxed);
    while lsn > cur {
        match WAL_FLUSHED_LSN.compare_exchange_weak(
            cur,
            lsn,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => break,
            Err(v) => cur = v,
        }
    }
}

// NEW: пороговые fsync (вне явного батча)
pub fn record_wal_threshold_flush(pages: u64, bytes: u64) {
    WAL_THRESHOLD_FLUSHES.fetch_add(1, Ordering::Relaxed);
    WAL_THRESHOLD_FLUSH_PAGES.fetch_add(pages, Ordering::Relaxed);
    WAL_THRESHOLD_FLUSH_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

// ----- Recorders (Page cache) -----
pub fn record_cache_hit() {
    PAGE_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
}
pub fn record_cache_miss() {
    PAGE_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
}

// ----- Recorders (In‑memory keydir fast‑path) -----
pub fn record_keydir_hit() {
    KEYDIR_HITS.fetch_add(1, Ordering::Relaxed);
}
pub fn record_keydir_miss() {
    KEYDIR_MISSES.fetch_add(1, Ordering::Relaxed);
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
    SNAPSHOTS_ACTIVE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| Some(v.saturating_sub(1)))
        .ok();
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

// NEW: fallback-скан под снапшотом
pub fn record_snapshot_fallback_scan() {
    SNAPSHOT_FALLBACK_SCANS.fetch_add(1, Ordering::Relaxed);
}

// NEW: TTL read-side skip
pub fn record_ttl_skipped() {
    TTL_SKIPPED.fetch_add(1, Ordering::Relaxed);
}

// NEW: Bloom fast-path recorders
pub fn record_bloom_negative() {
    BLOOM_TESTS.fetch_add(1, Ordering::Relaxed);
    BLOOM_NEGATIVE.fetch_add(1, Ordering::Relaxed);
}
pub fn record_bloom_positive() {
    BLOOM_TESTS.fetch_add(1, Ordering::Relaxed);
    BLOOM_POSITIVE.fetch_add(1, Ordering::Relaxed);
}
pub fn record_bloom_skipped_stale() {
    BLOOM_SKIPPED_STALE.fetch_add(1, Ordering::Relaxed);
}

// NEW: Bloom delta-update recorder
pub fn record_bloom_update(bytes: u64) {
    BLOOM_UPDATES_TOTAL.fetch_add(1, Ordering::Relaxed);
    BLOOM_UPDATE_BYTES.fetch_add(bytes, Ordering::Relaxed);
}

// ----- Recorders (Packing) -----
pub fn record_pack_page(slots: u64) {
    PACK_PAGES.fetch_add(1, Ordering::Relaxed);
    PACK_RECORDS.fetch_add(slots, Ordering::Relaxed);
    if slots == 1 {
        PACK_PAGES_SINGLE.fetch_add(1, Ordering::Relaxed);
    }
}

// ----- Recorders (Lazy compaction) -----
pub fn record_lazy_compact_run(pages_written: u64) {
    LAZY_COMPACT_RUNS.fetch_add(1, Ordering::Relaxed);
    LAZY_COMPACT_PAGES_WRITTEN.fetch_add(pages_written, Ordering::Relaxed);
}

// ----- Snapshot / Reset -----
pub fn snapshot() -> MetricsSnapshot {
    MetricsSnapshot {
        wal_appends_total: WAL_APPENDS_TOTAL.load(Ordering::Relaxed),
        wal_bytes_written: WAL_BYTES_WRITTEN.load(Ordering::Relaxed),
        wal_fsync_calls: WAL_FSYNC_CALLS.load(Ordering::Relaxed),
        wal_fsync_batch_pages: WAL_FSYNC_BATCH_PAGES.load(Ordering::Relaxed),
        wal_truncations: WAL_TRUNCATIONS.load(Ordering::Relaxed),

        // NEW
        wal_pending_max_lsn: WAL_PENDING_MAX_LSN.load(Ordering::Relaxed),
        wal_flushed_lsn: WAL_FLUSHED_LSN.load(Ordering::Relaxed),

        // NEW: threshold
        wal_threshold_flushes: WAL_THRESHOLD_FLUSHES.load(Ordering::Relaxed),
        wal_threshold_flush_pages: WAL_THRESHOLD_FLUSH_PAGES.load(Ordering::Relaxed),
        wal_threshold_flush_bytes: WAL_THRESHOLD_FLUSH_BYTES.load(Ordering::Relaxed),

        page_cache_hits: PAGE_CACHE_HITS.load(Ordering::Relaxed),
        page_cache_misses: PAGE_CACHE_MISSES.load(Ordering::Relaxed),

        // NEW
        keydir_hits: KEYDIR_HITS.load(Ordering::Relaxed),
        keydir_misses: KEYDIR_MISSES.load(Ordering::Relaxed),

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

        snapshot_fallback_scans: SNAPSHOT_FALLBACK_SCANS.load(Ordering::Relaxed),

        ttl_skipped: TTL_SKIPPED.load(Ordering::Relaxed),

        bloom_tests: BLOOM_TESTS.load(Ordering::Relaxed),
        bloom_negative: BLOOM_NEGATIVE.load(Ordering::Relaxed),
        bloom_positive: BLOOM_POSITIVE.load(Ordering::Relaxed),
        bloom_skipped_stale: BLOOM_SKIPPED_STALE.load(Ordering::Relaxed),

        // NEW
        bloom_updates_total: BLOOM_UPDATES_TOTAL.load(Ordering::Relaxed),
        bloom_update_bytes: BLOOM_UPDATE_BYTES.load(Ordering::Relaxed),

        // NEW: packing
        pack_pages: PACK_PAGES.load(Ordering::Relaxed),
        pack_records: PACK_RECORDS.load(Ordering::Relaxed),
        pack_pages_single: PACK_PAGES_SINGLE.load(Ordering::Relaxed),

        // NEW: lazy compact
        lazy_compact_runs: LAZY_COMPACT_RUNS.load(Ordering::Relaxed),
        lazy_compact_pages_written: LAZY_COMPACT_PAGES_WRITTEN.load(Ordering::Relaxed),
    }
}

pub fn reset() {
    WAL_APPENDS_TOTAL.store(0, Ordering::Relaxed);
    WAL_BYTES_WRITTEN.store(0, Ordering::Relaxed);
    WAL_FSYNC_CALLS.store(0, Ordering::Relaxed);
    WAL_FSYNC_BATCH_PAGES.store(0, Ordering::Relaxed);
    WAL_TRUNCATIONS.store(0, Ordering::Relaxed);

    // NEW
    WAL_PENDING_MAX_LSN.store(0, Ordering::Relaxed);
    WAL_FLUSHED_LSN.store(0, Ordering::Relaxed);

    // NEW: threshold
    WAL_THRESHOLD_FLUSHES.store(0, Ordering::Relaxed);
    WAL_THRESHOLD_FLUSH_PAGES.store(0, Ordering::Relaxed);
    WAL_THRESHOLD_FLUSH_BYTES.store(0, Ordering::Relaxed);

    PAGE_CACHE_HITS.store(0, Ordering::Relaxed);
    PAGE_CACHE_MISSES.store(0, Ordering::Relaxed);

    // NEW: keydir fast-path
    KEYDIR_HITS.store(0, Ordering::Relaxed);
    KEYDIR_MISSES.store(0, Ordering::Relaxed);

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

    SNAPSHOT_FALLBACK_SCANS.store(0, Ordering::Relaxed);

    TTL_SKIPPED.store(0, Ordering::Relaxed);

    BLOOM_TESTS.store(0, Ordering::Relaxed);
    BLOOM_NEGATIVE.store(0, Ordering::Relaxed);
    BLOOM_POSITIVE.store(0, Ordering::Relaxed);
    BLOOM_SKIPPED_STALE.store(0, Ordering::Relaxed);

    // NEW: bloom delta-update
    BLOOM_UPDATES_TOTAL.store(0, Ordering::Relaxed);
    BLOOM_UPDATE_BYTES.store(0, Ordering::Relaxed);

    // NEW: packing
    PACK_PAGES.store(0, Ordering::Relaxed);
    PACK_RECORDS.store(0, Ordering::Relaxed);
    PACK_PAGES_SINGLE.store(0, Ordering::Relaxed);

    // NEW: lazy compact
    LAZY_COMPACT_RUNS.store(0, Ordering::Relaxed);
    LAZY_COMPACT_PAGES_WRITTEN.store(0, Ordering::Relaxed);
}