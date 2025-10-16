use anyhow::{anyhow, Result};
use clap::Parser;
use tiny_http::{Header, Response, Server};

use std::path::PathBuf;

use QuiverDB::{dir::Directory, meta::read_meta, metrics};
// Bloom side-car + cache (только верхнеуровневые реэкспорты)
use QuiverDB::bloom::{bloom_cache_stats, bloom_cache_counters};
// Page cache diagnostics
use QuiverDB::pager::cache::{
    page_cache_evictions_total,
    page_cache_invalidations_total,  // NEW
    page_cache_len
};

#[derive(Parser, Debug)]
#[command(
    name = "quiverdb_metrics",
    version,
    about = "QuiverDB 2.x metrics exporter (Prometheus)"
)]
struct Opt {
    #[arg(long, default_value = "0.0.0.0:9898")]
    addr: String,
    #[arg(long)]
    path: Option<PathBuf>,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {:#}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let opt = Opt::parse();

    let server = Server::http(&opt.addr)
        .map_err(|e| anyhow!("bind http at {}: {}", opt.addr, e))?;
    println!("quiverdb_metrics listening on {}", opt.addr);

    loop {
        let rq = match server.recv() {
            Ok(rq) => rq,
            Err(e) => {
                eprintln!("http recv error: {}", e);
                continue;
            }
        };

        let url = rq.url().to_string();
        let method = rq.method().as_str().to_string();

        if method == "GET" && (url == "/" || url == "/health" || url == "/ready") {
            let resp = Response::from_string("OK\n").with_status_code(200);
            let _ = rq.respond(resp);
            continue;
        }

        if method == "GET" && url == "/metrics" {
            let body = build_metrics(&opt.path).unwrap_or_else(|e| {
                format!(
                    "# exporter error\nquiverdb_exporter_error 1\n# msg\n# {}\n",
                    e
                )
            });
            let mut resp = Response::from_string(body);
            if let Ok(ct) = Header::from_bytes(b"Content-Type", b"text/plain; version=0.0.4") {
                resp.add_header(ct);
            }
            let _ = rq.respond(resp);
            continue;
        }

        let resp = Response::from_string("not found\n").with_status_code(404);
        let _ = rq.respond(resp);
    }
}

fn build_metrics(path: &Option<PathBuf>) -> Result<String> {
    let m = metrics::snapshot();
    let mut out = String::new();

    let ver = env!("CARGO_PKG_VERSION");
    out.push_str("# HELP quiverdb_build_info Build info.\n");
    out.push_str("# TYPE quiverdb_build_info gauge\n");
    out.push_str(&format!("quiverdb_build_info{{version=\"{}\"}} 1\n", ver));

    // --- WAL core ---
    out.push_str("# HELP quiverdb_wal_appends_total Total WAL appends.\n");
    out.push_str("# TYPE quiverdb_wal_appends_total counter\n");
    out.push_str(&format!("quiverdb_wal_appends_total {}\n", m.wal_appends_total));

    out.push_str("# HELP quiverdb_wal_bytes_written Total WAL bytes written.\n");
    out.push_str("# TYPE quiverdb_wal_bytes_written counter\n");
    out.push_str(&format!("quiverdb_wal_bytes_written {}\n", m.wal_bytes_written));

    out.push_str("# HELP quiverdb_wal_fsync_calls WAL fsync calls.\n");
    out.push_str("# TYPE quiverdb_wal_fsync_calls counter\n");
    out.push_str(&format!("quiverdb_wal_fsync_calls {}\n", m.wal_fsync_calls));

    out.push_str("# HELP quiverdb_wal_fsync_batch_pages Average pages per WAL fsync.\n");
    out.push_str("# TYPE quiverdb_wal_fsync_batch_pages gauge\n");
    out.push_str(&format!("quiverdb_wal_fsync_batch_pages {:.2}\n", m.avg_wal_batch_pages()));

    out.push_str("# HELP quiverdb_wal_truncations WAL file truncations.\n");
    out.push_str("# TYPE quiverdb_wal_truncations counter\n");
    out.push_str(&format!("quiverdb_wal_truncations {}\n", m.wal_truncations));

    out.push_str("# HELP quiverdb_wal_pending_max_lsn Max LSN of PAGE_IMAGE written but not yet fsynced.\n");
    out.push_str("# TYPE quiverdb_wal_pending_max_lsn gauge\n");
    out.push_str(&format!("quiverdb_wal_pending_max_lsn {}\n", m.wal_pending_max_lsn));

    out.push_str("# HELP quiverdb_wal_flushed_lsn Last LSN durably fsynced in WAL.\n");
    out.push_str("# TYPE quiverdb_wal_flushed_lsn gauge\n");
    out.push_str(&format!("quiverdb_wal_flushed_lsn {}\n", m.wal_flushed_lsn));

    // --- WAL threshold flush (new) ---
    out.push_str("# HELP quiverdb_wal_threshold_flushes_total Threshold-based WAL fsyncs (outside explicit batches).\n");
    out.push_str("# TYPE quiverdb_wal_threshold_flushes_total counter\n");
    out.push_str(&format!("quiverdb_wal_threshold_flushes_total {}\n", m.wal_threshold_flushes));

    out.push_str("# HELP quiverdb_wal_threshold_flush_pages_total Pages accumulated when threshold fsync triggered.\n");
    out.push_str("# TYPE quiverdb_wal_threshold_flush_pages_total counter\n");
    out.push_str(&format!("quiverdb_wal_threshold_flush_pages_total {}\n", m.wal_threshold_flush_pages));

    out.push_str("# HELP quiverdb_wal_threshold_flush_bytes_total Bytes accumulated when threshold fsync triggered.\n");
    out.push_str("# TYPE quiverdb_wal_threshold_flush_bytes_total counter\n");
    out.push_str(&format!("quiverdb_wal_threshold_flush_bytes_total {}\n", m.wal_threshold_flush_bytes));

    // --- Page cache ---
    out.push_str("# HELP quiverdb_page_cache_hits Page cache hits.\n");
    out.push_str("# TYPE quiverdb_page_cache_hits counter\n");
    out.push_str(&format!("quiverdb_page_cache_hits {}\n", m.page_cache_hits));

    out.push_str("# HELP quiverdb_page_cache_misses Page cache misses.\n");
    out.push_str("# TYPE quiverdb_page_cache_misses counter\n");
    out.push_str(&format!("quiverdb_page_cache_misses {}\n", m.page_cache_misses));

    out.push_str("# HELP quiverdb_page_cache_hit_ratio Page cache hit ratio (percent).\n");
    out.push_str("# TYPE quiverdb_page_cache_hit_ratio gauge\n");
    out.push_str(&format!("quiverdb_page_cache_hit_ratio {:.2}\n", m.cache_hit_ratio() * 100.0));

    let pc_len = page_cache_len() as u64;
    let pc_ev = page_cache_evictions_total();
    let pc_inv = page_cache_invalidations_total(); // NEW

    out.push_str("# HELP quiverdb_page_cache_len Current number of pages cached.\n");
    out.push_str("# TYPE quiverdb_page_cache_len gauge\n");
    out.push_str(&format!("quiverdb_page_cache_len {}\n", pc_len));

    out.push_str("# HELP quiverdb_page_cache_evictions_total Evicted pages from page cache since start (or last reconfigure/clear).\n");
    out.push_str("# TYPE quiverdb_page_cache_evictions_total counter\n");
    out.push_str(&format!("quiverdb_page_cache_evictions_total {}\n", pc_ev));

    // NEW: invalidations metric
    out.push_str("# HELP quiverdb_page_cache_invalidations_total Explicit invalidations of page cache entries since start (or last reconfigure/clear).\n");
    out.push_str("# TYPE quiverdb_page_cache_invalidations_total counter\n");
    out.push_str(&format!("quiverdb_page_cache_invalidations_total {}\n", pc_inv));

    // --- Keydir fast-path ---
    out.push_str("# HELP quiverdb_keydir_hits In-memory keydir fast-path hits.\n");
    out.push_str("# TYPE quiverdb_keydir_hits counter\n");
    out.push_str(&format!("quiverdb_keydir_hits {}\n", m.keydir_hits));

    out.push_str("# HELP quiverdb_keydir_misses In-memory keydir fast-path misses.\n");
    out.push_str("# TYPE quiverdb_keydir_misses counter\n");
    out.push_str(&format!("quiverdb_keydir_misses {}\n", m.keydir_misses));

    out.push_str("# HELP quiverdb_keydir_hit_ratio In-memory keydir hit ratio (percent).\n");
    out.push_str("# TYPE quiverdb_keydir_hit_ratio gauge\n");
    out.push_str(&format!("quiverdb_keydir_hit_ratio {:.2}\n", m.keydir_hit_ratio() * 100.0));

    // --- Robin Hood ---
    out.push_str("# HELP quiverdb_rh_page_compactions Robin Hood in-page compactions.\n");
    out.push_str("# TYPE quiverdb_rh_page_compactions counter\n");
    out.push_str(&format!("quiverdb_rh_page_compactions {}\n", m.rh_page_compactions));

    // --- Overflow ---
    out.push_str("# HELP quiverdb_overflow_chains_created Overflow chains created.\n");
    out.push_str("# TYPE quiverdb_overflow_chains_created counter\n");
    out.push_str(&format!("quiverdb_overflow_chains_created {}\n", m.overflow_chains_created));

    out.push_str("# HELP quiverdb_overflow_chains_freed Overflow chains freed.\n");
    out.push_str("# TYPE quiverdb_overflow_chains_freed counter\n");
    out.push_str(&format!("quiverdb_overflow_chains_freed {}\n", m.overflow_chains_freed));

    // --- Maintenance ---
    out.push_str("# HELP quiverdb_sweep_orphan_runs Orphan sweep runs.\n");
    out.push_str("# TYPE quiverdb_sweep_orphan_runs counter\n");
    out.push_str(&format!("quiverdb_sweep_orphan_runs {}\n", m.sweep_orphan_runs));

    // --- Snapshots / Backup / Restore ---
    out.push_str("# HELP quiverdb_snapshots_active Active snapshots.\n");
    out.push_str("# TYPE quiverdb_snapshots_active gauge\n");
    out.push_str(&format!("quiverdb_snapshots_active {}\n", m.snapshots_active));

    out.push_str("# HELP quiverdb_snapshot_freeze_frames Snapshot freeze frames.\n");
    out.push_str("# TYPE quiverdb_snapshot_freeze_frames counter\n");
    out.push_str(&format!("quiverdb_snapshot_freeze_frames {}\n", m.snapshot_freeze_frames));

    out.push_str("# HELP quiverdb_snapshot_freeze_bytes Snapshot freeze bytes.\n");
    out.push_str("# TYPE quiverdb_snapshot_freeze_bytes counter\n");
    out.push_str(&format!("quiverdb_snapshot_freeze_bytes {}\n", m.snapshot_freeze_bytes));

    out.push_str("# HELP quiverdb_backup_pages_emitted Backup pages emitted.\n");
    out.push_str("# TYPE quiverdb_backup_pages_emitted counter\n");
    out.push_str(&format!("quiverdb_backup_pages_emitted {}\n", m.backup_pages_emitted));

    out.push_str("# HELP quiverdb_backup_bytes_emitted Backup bytes emitted.\n");
    out.push_str("# TYPE quiverdb_backup_bytes_emitted counter\n");
    out.push_str(&format!("quiverdb_backup_bytes_emitted {}\n", m.backup_bytes_emitted));

    out.push_str("# HELP quiverdb_restore_pages_written Restore pages written.\n");
    out.push_str("# TYPE quiverdb_restore_pages_written counter\n");
    out.push_str(&format!("quiverdb_restore_pages_written {}\n", m.restore_pages_written));

    out.push_str("# HELP quiverdb_restore_bytes_written Restore bytes written.\n");
    out.push_str("# TYPE quiverdb_restore_bytes_written counter\n");
    out.push_str(&format!("quiverdb_restore_bytes_written {}\n", m.restore_bytes_written));

    out.push_str("# HELP quiverdb_snapshot_fallback_scans Snapshot fallback scans.\n");
    out.push_str("# TYPE quiverdb_snapshot_fallback_scans counter\n");
    out.push_str(&format!("quiverdb_snapshot_fallback_scans {}\n", m.snapshot_fallback_scans));

    // --- TTL ---
    out.push_str("# HELP quiverdb_ttl_skipped TTL-based skipped reads.\n");
    out.push_str("# TYPE quiverdb_ttl_skipped counter\n");
    out.push_str(&format!("quiverdb_ttl_skipped {}\n", m.ttl_skipped));

    // --- Bloom ---
    out.push_str("# HELP quiverdb_bloom_tests Bloom tests performed (fresh filters only).\n");
    out.push_str("# TYPE quiverdb_bloom_tests counter\n");
    out.push_str(&format!("quiverdb_bloom_tests {}\n", m.bloom_tests));

    out.push_str("# HELP quiverdb_bloom_negative Bloom negative hints (definitely absent).\n");
    out.push_str("# TYPE quiverdb_bloom_negative counter\n");
    out.push_str(&format!("quiverdb_bloom_negative {}\n", m.bloom_negative));

    out.push_str("# HELP quiverdb_bloom_positive Bloom positive hints (maybe present).\n");
    out.push_str("# TYPE quiverdb_bloom_positive counter\n");
    out.push_str(&format!("quiverdb_bloom_positive {}\n", m.bloom_positive));

    out.push_str("# HELP quiverdb_bloom_skipped_stale Bloom skipped due to staleness or incompatibility.\n");
    out.push_str("# TYPE quiverdb_bloom_skipped_stale counter\n");
    out.push_str(&format!("quiverdb_bloom_skipped_stale {}\n", m.bloom_skipped_stale));

    let (bc_cap, bc_len) = bloom_cache_stats();
    let (bc_hits, bc_miss) = bloom_cache_counters();

    out.push_str("# HELP quiverdb_bloom_cache_capacity Bloom cache capacity (buckets).\n");
    out.push_str("# TYPE quiverdb_bloom_cache_capacity gauge\n");
    out.push_str(&format!("quiverdb_bloom_cache_capacity {}\n", bc_cap));

    out.push_str("# HELP quiverdb_bloom_cache_len Bloom cache entries currently stored (buckets cached).\n");
    out.push_str("# TYPE quiverdb_bloom_cache_len gauge\n");
    out.push_str(&format!("quiverdb_bloom_cache_len {}\n", bc_len));

    out.push_str("# HELP quiverdb_bloom_cache_hits Bloom cache hits.\n");
    out.push_str("# TYPE quiverdb_bloom_cache_hits counter\n");
    out.push_str(&format!("quiverdb_bloom_cache_hits {}\n", bc_hits));

    out.push_str("# HELP quiverdb_bloom_cache_misses Bloom cache misses.\n");
    out.push_str("# TYPE quiverdb_bloom_cache_misses counter\n");
    out.push_str(&format!("quiverdb_bloom_cache_misses {}\n", bc_miss));

    // --- Lazy compaction ---
    out.push_str("# HELP quiverdb_lazy_compact_runs Lazy compaction runs.\n");
    out.push_str("# TYPE quiverdb_lazy_compact_runs counter\n");
    out.push_str(&format!("quiverdb_lazy_compact_runs {}\n", m.lazy_compact_runs));

    out.push_str("# HELP quiverdb_lazy_compact_pages_written Pages written by lazy compaction.\n");
    out.push_str("# TYPE quiverdb_lazy_compact_pages_written counter\n");
    out.push_str(&format!("quiverdb_lazy_compact_pages_written {}\n", m.lazy_compact_pages_written));

    // --- Optional DB info from --path ---
    if let Some(root) = path {
        if root.exists() {
            if let Ok(meta) = read_meta(root) {
                out.push_str("# HELP quiverdb_page_size_bytes Page size (bytes).\n");
                out.push_str("# TYPE quiverdb_page_size_bytes gauge\n");
                out.push_str(&format!("quiverdb_page_size_bytes {}\n", meta.page_size));

                out.push_str("# HELP quiverdb_pages_allocated Total pages allocated.\n");
                out.push_str("# TYPE quiverdb_pages_allocated gauge\n");
                out.push_str(&format!("quiverdb_pages_allocated {}\n", meta.next_page_id));

                if let Ok(dir) = Directory::open(root) {
                    out.push_str("# HELP quiverdb_buckets Buckets in directory.\n");
                    out.push_str("# TYPE quiverdb_buckets gauge\n");
                    out.push_str(&format!("quiverdb_buckets {}\n", dir.bucket_count));

                    let used = dir.count_used_buckets().unwrap_or(0);
                    out.push_str("# HELP quiverdb_used_buckets Used buckets.\n");
                    out.push_str("# TYPE quiverdb_used_buckets gauge\n");
                    out.push_str(&format!("quiverdb_used_buckets {}\n", used));
                }
            }
        }
    }

    Ok(out)
}