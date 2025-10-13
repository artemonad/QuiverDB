use anyhow::Result;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::dir::Directory;
use QuiverDB::meta::read_meta;
// Bloom side-car status + cache counters (через реэкспорт)
use QuiverDB::bloom::{BloomSidecar, bloom_cache_stats, bloom_cache_counters};
// Page cache diagnostics
use QuiverDB::pager::cache::{page_cache_len, page_cache_evictions_total};
// metrics snapshot
use QuiverDB::metrics;
// Key journal (TDE epochs)
use QuiverDB::crypto::KeyJournal;

/// New: JSON-aware status (when json=true prints one JSON object).
pub fn exec_with_json(path: PathBuf, json: bool) -> Result<()> {
    let m = read_meta(&path)?;

    // RO-открытие для статуса каталога/ускорителей и печати статистики
    let db = Db::open_ro(&path)?;

    // Directory quick info
    let dir = Directory::open(&path)?;
    let used_buckets = dir.count_used_buckets().unwrap_or(0);

    // TDE
    let tde_enabled = db.pager.tde_enabled();
    let tde_kid = db.pager.tde_kid().map(|s| s.to_string());
    let tde_mode = if tde_enabled { "aead" } else { "crc" };
    let tde_key_loaded = db.pager.tde_key_loaded();

    // TDE key journal (epochs)
    let tde_epochs: Vec<(u64, String)> = match KeyJournal::open(&path) {
        Ok(j) => j.epochs().unwrap_or_default(),
        Err(_) => Vec::new(),
    };

    // Acceleration: mem_keydir
    let mem_keydir_present = db.has_mem_keydir();

    // Bloom status + meta + cache stats
    let (bloom_present, bloom_buckets, bloom_fresh, bloom_bpb, bloom_k, bloom_last_lsn) =
        match BloomSidecar::open_ro(&path) {
            Ok(sidecar) => (
                true,
                Some(sidecar.buckets()),
                Some(sidecar.is_fresh_for_db(&db)),
                Some(sidecar.bytes_per_bucket()),
                Some(sidecar.k_hashes()),
                Some(sidecar.last_lsn()),
            ),
            Err(_) => (false, None, None, None, None, None),
        };
    let (bloom_cache_cap, bloom_cache_entries) = bloom_cache_stats();
    let (bloom_cache_hits, bloom_cache_misses) = bloom_cache_counters();

    if json {
        // Metrics snapshot
        let ms = metrics::snapshot();

        // Page cache diagnostics (live)
        let pc_len = page_cache_len() as u64;
        let pc_ev = page_cache_evictions_total();

        print!("{{");
        // meta
        print!("\"meta\":{{");
        print!("\"version\":{},", m.version);
        print!("\"page_size\":{},", m.page_size);
        print!("\"flags\":{},", m.flags);
        print!("\"hash_kind\":{},", m.hash_kind);
        print!("\"checksum_kind\":{},", m.checksum_kind);
        print!("\"codec_default\":{},", m.codec_default);
        print!("\"next_page_id\":{},", m.next_page_id);
        print!("\"last_lsn\":{},", m.last_lsn);
        print!("\"clean_shutdown\":{}", m.clean_shutdown);
        print!("}},"); // meta

        // tde
        print!("\"tde\":{{");
        print!("\"enabled\":{}", tde_enabled);
        print!(",\"mode\":\"{}\"", tde_mode);
        print!(",\"key_loaded\":{}", tde_key_loaded);
        print!(",\"kid\":");
        match tde_kid {
            Some(ref s) => print!("\"{}\"", escape_json(s)),
            None => print!("null"),
        }
        // epochs JSON
        print!(",\"epochs\":[");
        for (i, (since, kid)) in tde_epochs.iter().enumerate() {
            if i > 0 {
                print!(",");
            }
            print!("{{\"since_lsn\":{},\"kid\":\"{}\"}}", since, escape_json(kid));
        }
        print!("]");
        print!("}},"); // tde

        // acceleration
        print!("\"acceleration\":{{");
        print!("\"mem_keydir\":{}", mem_keydir_present);
        print!("}},"); // acceleration

        // bloom
        print!("\"bloom\":{{");
        print!("\"present\":{}", bloom_present);
        if let Some(b) = bloom_buckets {
            print!(",\"buckets\":{}", b);
        } else {
            print!(",\"buckets\":null");
        }
        if let Some(f) = bloom_fresh {
            print!(",\"fresh\":{}", f);
        } else {
            print!(",\"fresh\":null");
        }
        if let Some(bpb) = bloom_bpb {
            print!(",\"bytes_per_bucket\":{}", bpb);
        } else {
            print!(",\"bytes_per_bucket\":null");
        }
        if let Some(k) = bloom_k {
            print!(",\"k_hashes\":{}", k);
        } else {
            print!(",\"k_hashes\":null");
        }
        if let Some(lsn) = bloom_last_lsn {
            print!(",\"last_lsn\":{}", lsn);
        } else {
            print!(",\"last_lsn\":null");
        }
        // cache stats (глобальные)
        print!(",\"cache_cap\":{}", bloom_cache_cap);
        print!(",\"cache_entries\":{}", bloom_cache_entries);
        // new: bloom cache counters
        print!(",\"cache_hits\":{}", bloom_cache_hits);
        print!(",\"cache_misses\":{}", bloom_cache_misses);
        print!("}},"); // bloom

        // directory summary
        print!("\"directory\":{{");
        print!("\"buckets\":{},", dir.bucket_count);
        print!("\"used_buckets\":{}", used_buckets);
        print!("}},"); // directory

        // metrics snapshot (включая packing и bloom delta-update)
        print!("\"metrics\":{{");
        print!("\"wal_appends_total\":{},", ms.wal_appends_total);
        print!("\"wal_bytes_written\":{},", ms.wal_bytes_written);
        print!("\"wal_fsync_calls\":{},", ms.wal_fsync_calls);
        print!("\"wal_avg_batch_pages\":{:.2},", ms.avg_wal_batch_pages());
        print!("\"wal_truncations\":{},", ms.wal_truncations);
        print!("\"wal_pending_max_lsn\":{},", ms.wal_pending_max_lsn);
        print!("\"wal_flushed_lsn\":{},", ms.wal_flushed_lsn);

        print!("\"page_cache_hits\":{},", ms.page_cache_hits);
        print!("\"page_cache_misses\":{},", ms.page_cache_misses);
        print!("\"page_cache_hit_ratio\":{:.2},", ms.cache_hit_ratio());
        // NEW: page cache diagnostics
        print!("\"page_cache_len\":{},", pc_len);
        print!("\"page_cache_evictions_total\":{},", pc_ev);

        // NEW: keydir fast-path
        print!("\"keydir_hits\":{},", ms.keydir_hits);
        print!("\"keydir_misses\":{},", ms.keydir_misses);
        print!("\"keydir_hit_ratio\":{:.2},", ms.keydir_hit_ratio());

        print!("\"rh_page_compactions\":{},", ms.rh_page_compactions);

        print!("\"overflow_chains_created\":{},", ms.overflow_chains_created);
        print!("\"overflow_chains_freed\":{},", ms.overflow_chains_freed);

        print!("\"sweep_orphan_runs\":{},", ms.sweep_orphan_runs);

        print!("\"snapshots_active\":{},", ms.snapshots_active);
        print!("\"snapshot_freeze_frames\":{},", ms.snapshot_freeze_frames);
        print!("\"snapshot_freeze_bytes\":{},", ms.snapshot_freeze_bytes);

        print!("\"backup_pages_emitted\":{},", ms.backup_pages_emitted);
        print!("\"backup_bytes_emitted\":{},", ms.backup_bytes_emitted);
        print!("\"restore_pages_written\":{},", ms.restore_pages_written);
        print!("\"restore_bytes_written\":{},", ms.restore_bytes_written);

        print!("\"snapshot_fallback_scans\":{},", ms.snapshot_fallback_scans);

        print!("\"ttl_skipped\":{},", ms.ttl_skipped);

        print!("\"bloom_tests\":{},", ms.bloom_tests);
        print!("\"bloom_negative\":{},", ms.bloom_negative);
        print!("\"bloom_positive\":{},", ms.bloom_positive);
        print!("\"bloom_skipped_stale\":{},", ms.bloom_skipped_stale);

        // packing metrics
        print!("\"pack_pages\":{},", ms.pack_pages);
        print!("\"pack_records\":{},", ms.pack_records);
        print!("\"pack_pages_single\":{},", ms.pack_pages_single);
        print!("\"pack_avg_records_per_page\":{:.3},", ms.avg_pack_records_per_page());

        // NEW: bloom delta-update metrics
        print!("\"bloom_updates_total\":{},", ms.bloom_updates_total);
        print!("\"bloom_update_bytes\":{},", ms.bloom_update_bytes);

        // NEW: lazy compact metrics
        print!("\"lazy_compact_runs\":{},", ms.lazy_compact_runs);
        print!("\"lazy_compact_pages_written\":{}", ms.lazy_compact_pages_written);

        print!("}}"); // metrics

        println!("}}");
        return Ok(());
    }

    // Human-readable (old behavior + Bloom)
    println!("DB {}", path.display());
    println!("  version        = {}", m.version);
    println!("  page_size      = {}", m.page_size);
    println!("  flags          = 0x{:08x}", m.flags);
    println!("  hash_kind      = {}", m.hash_kind);
    println!("  checksum_kind  = {}", m.checksum_kind);
    println!("  codec_default  = {}", m.codec_default);
    println!("  next_page_id   = {}", m.next_page_id);
    println!("  last_lsn       = {}", m.last_lsn);
    println!("  clean_shutdown = {}", m.clean_shutdown);

    // TDE статус
    println!("TDE:");
    println!("  enabled        = {}", tde_enabled);
    println!("  mode           = {}", tde_mode);
    println!("  key_loaded     = {}", tde_key_loaded);
    println!(
        "  kid            = {}",
        tde_kid.as_deref().unwrap_or("(default/provider)")
    );
    if !tde_epochs.is_empty() {
        println!("  epochs total   = {}", tde_epochs.len());
        if let Some((since, kid)) = tde_epochs.last() {
            println!("  last_epoch     = (since_lsn={}, kid={})", since, kid);
        }
    }

    // In-memory keydir (ускоритель get/exists/scan)
    println!("Acceleration:");
    println!(
        "  mem_keydir     = {}",
        if mem_keydir_present { "present" } else { "absent" }
    );

    // Bloom side-car
    println!("Bloom:");
    if bloom_present {
        println!("  present          = true");
        println!("  buckets          = {}", bloom_buckets.unwrap_or(0));
        println!("  fresh            = {}", bloom_fresh.unwrap_or(false));
        // meta
        if let Some(bpb) = bloom_bpb {
            println!("  bytes_per_bucket = {}", bpb);
        }
        if let Some(k) = bloom_k {
            println!("  k_hashes         = {}", k);
        }
        if let Some(lsn) = bloom_last_lsn {
            println!("  last_lsn         = {}", lsn);
        }
    } else {
        println!("  present          = false");
    }
    // cache stats
    println!("  cache_cap        = {}", bloom_cache_cap);
    println!("  cache_entries    = {}", bloom_cache_entries);
    // new: counters
    println!("  cache_hits       = {}", bloom_cache_hits);
    println!("  cache_misses     = {}", bloom_cache_misses);

    // NEW: Bloom updates (delta)
    let ms = metrics::snapshot();

    // Metrics snapshot (human)
    println!("Metrics snapshot:");
    println!("  wal_appends_total       = {}", ms.wal_appends_total);
    println!("  wal_bytes_written       = {}", ms.wal_bytes_written);
    println!("  wal_fsync_calls         = {}", ms.wal_fsync_calls);
    println!("  wal_avg_batch_pages     = {:.2}", ms.avg_wal_batch_pages());
    println!("  wal_truncations         = {}", ms.wal_truncations);
    println!("  wal_pending_max_lsn     = {}", ms.wal_pending_max_lsn);
    println!("  wal_flushed_lsn         = {}", ms.wal_flushed_lsn);

    println!("  page_cache_hits         = {}", ms.page_cache_hits);
    println!("  page_cache_misses       = {}", ms.page_cache_misses);
    println!("  page_cache_hit_ratio    = {:.2}%", ms.cache_hit_ratio() * 100.0);
    // NEW: diagnostics
    println!("  page_cache_len          = {}", page_cache_len());
    println!(
        "  page_cache_evictions    = {}",
        page_cache_evictions_total()
    );

    // NEW: keydir fast-path
    println!("  keydir_hits             = {}", ms.keydir_hits);
    println!("  keydir_misses           = {}", ms.keydir_misses);
    println!("  keydir_hit_ratio        = {:.2}%", ms.keydir_hit_ratio() * 100.0);

    println!("  rh_page_compactions     = {}", ms.rh_page_compactions);

    println!("  overflow_chains_created = {}", ms.overflow_chains_created);
    println!("  overflow_chains_freed   = {}", ms.overflow_chains_freed);

    println!("  sweep_orphan_runs       = {}", ms.sweep_orphan_runs);

    println!("  snapshots_active        = {}", ms.snapshots_active);
    println!("  snapshot_freeze_frames  = {}", ms.snapshot_freeze_frames);
    println!("  snapshot_freeze_bytes   = {}", ms.snapshot_freeze_bytes);
    println!("  backup_pages_emitted    = {}", ms.backup_pages_emitted);
    println!("  backup_bytes_emitted    = {}", ms.backup_bytes_emitted);
    println!("  restore_pages_written   = {}", ms.restore_pages_written);
    println!("  restore_bytes_written   = {}", ms.restore_bytes_written);
    println!("  snapshot_fallback_scans = {}", ms.snapshot_fallback_scans);

    println!("  ttl_skipped             = {}", ms.ttl_skipped);

    println!(
        "  bloom tests/neg/pos/skip= {}/{}/{}/{}",
        ms.bloom_tests, ms.bloom_negative, ms.bloom_positive, ms.bloom_skipped_stale
    );
    // NEW: packing metrics
    println!("  pack_pages              = {}", ms.pack_pages);
    println!("  pack_records            = {}", ms.pack_records);
    println!("  pack_pages_single       = {}", ms.pack_pages_single);
    println!("  pack_avg_records/page   = {:.2}", ms.avg_pack_records_per_page());

    // NEW: lazy compact metrics
    println!("  lazy_compact_runs       = {}", ms.lazy_compact_runs);
    println!("  lazy_compact_pages_written = {}", ms.lazy_compact_pages_written);

    // Keep original stats printer (text or JSON via P1_DBSTATS_JSON)
    db.print_stats()?;
    Ok(())
}

// ---------- helpers ----------

fn escape_json(s: &str) -> String {
    // простая экранизация: заменим \ и " (достаточно для наших целей)
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\"' => out.push_str("\\\""),
            c => out.push(c),
        }
    }
    out
}