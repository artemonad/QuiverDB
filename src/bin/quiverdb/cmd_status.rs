use anyhow::Result;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::dir::Directory;
use QuiverDB::meta::read_meta;
// Bloom side-car status + cache counters (через реэкспорт)
use QuiverDB::bloom::{bloom_cache_counters, bloom_cache_stats, BloomSidecar};
// Page cache diagnostics
use QuiverDB::pager::cache::{
    page_cache_evictions_total, page_cache_invalidations_total, page_cache_len,
};
// metrics snapshot
use QuiverDB::metrics;
// Key journal (TDE epochs)
use QuiverDB::crypto::KeyJournal;

// serde_json для безопасного JSON-вывода
use serde_json::json;

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
    let bloom_sidecar_ro = BloomSidecar::open_ro(&path);
    let (bloom_present, bloom_buckets, bloom_fresh, bloom_bpb, bloom_k, bloom_last_lsn) =
        match bloom_sidecar_ro {
            Ok(ref sidecar) => (
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

    // Bloom stale reason
    let bloom_reason = if !bloom_present {
        "absent".to_string()
    } else if let Some(b) = bloom_buckets {
        if b != dir.bucket_count {
            "buckets_mismatch".to_string()
        } else if let Some(lsn) = bloom_last_lsn {
            if lsn != db.pager.meta.last_lsn {
                "stale_last_lsn".to_string()
            } else {
                "fresh".to_string()
            }
        } else {
            "stale_last_lsn".to_string()
        }
    } else {
        "absent".to_string()
    };

    if json {
        // Metrics snapshot
        let ms = metrics::snapshot();

        // Page cache diagnostics (live)
        let pc_len = page_cache_len() as u64;
        let pc_ev = page_cache_evictions_total();
        let pc_inv = page_cache_invalidations_total();

        // Сформируем JSON объект
        let tde_epochs_json = tde_epochs
            .iter()
            .map(|(since, kid)| json!({"since_lsn": since, "kid": kid}))
            .collect::<Vec<_>>();

        let status = json!({
            "meta": {
                "version": m.version,
                "page_size": m.page_size,
                "flags": m.flags,
                "hash_kind": m.hash_kind,
                "checksum_kind": m.checksum_kind,
                "codec_default": m.codec_default,
                "next_page_id": m.next_page_id,
                "last_lsn": m.last_lsn,
                "clean_shutdown": m.clean_shutdown
            },
            "tde": {
                "enabled": tde_enabled,
                "mode": tde_mode,
                "key_loaded": tde_key_loaded,
                "kid": tde_kid,
                "epochs": tde_epochs_json
            },
            "acceleration": {
                "mem_keydir": mem_keydir_present
            },
            "bloom": {
                "present": bloom_present,
                "buckets": bloom_buckets,
                "fresh": bloom_fresh,
                "bytes_per_bucket": bloom_bpb,
                "k_hashes": bloom_k,
                "last_lsn": bloom_last_lsn,
                "cache_cap": bloom_cache_cap,
                "cache_entries": bloom_cache_entries,
                "cache_hits": bloom_cache_hits,
                "cache_misses": bloom_cache_misses,
                "reason": bloom_reason,
                "expected_buckets": dir.bucket_count,
                "expected_last_lsn": db.pager.meta.last_lsn
            },
            "directory": {
                "buckets": dir.bucket_count,
                "used_buckets": used_buckets
            },
            "metrics": {
                "wal_appends_total": ms.wal_appends_total,
                "wal_bytes_written": ms.wal_bytes_written,
                "wal_fsync_calls": ms.wal_fsync_calls,
                "wal_avg_batch_pages": ms.avg_wal_batch_pages(),
                "wal_truncations": ms.wal_truncations,
                "wal_pending_max_lsn": ms.wal_pending_max_lsn,
                "wal_flushed_lsn": ms.wal_flushed_lsn,

                "page_cache_hits": ms.page_cache_hits,
                "page_cache_misses": ms.page_cache_misses,
                "page_cache_hit_ratio": ms.cache_hit_ratio(),
                "page_cache_len": pc_len,
                "page_cache_evictions_total": pc_ev,
                "page_cache_invalidations_total": pc_inv,

                "keydir_hits": ms.keydir_hits,
                "keydir_misses": ms.keydir_misses,
                "keydir_hit_ratio": ms.keydir_hit_ratio(),

                "rh_page_compactions": ms.rh_page_compactions,

                "overflow_chains_created": ms.overflow_chains_created,
                "overflow_chains_freed": ms.overflow_chains_freed,

                "sweep_orphan_runs": ms.sweep_orphan_runs,

                "snapshots_active": ms.snapshots_active,
                "snapshot_freeze_frames": ms.snapshot_freeze_frames,
                "snapshot_freeze_bytes": ms.snapshot_freeze_bytes,
                "backup_pages_emitted": ms.backup_pages_emitted,
                "backup_bytes_emitted": ms.backup_bytes_emitted,
                "restore_pages_written": ms.restore_pages_written,
                "restore_bytes_written": ms.restore_bytes_written,

                "snapshot_fallback_scans": ms.snapshot_fallback_scans,

                "ttl_skipped": ms.ttl_skipped,

                "bloom_tests": ms.bloom_tests,
                "bloom_negative": ms.bloom_negative,
                "bloom_positive": ms.bloom_positive,
                "bloom_skipped_stale": ms.bloom_skipped_stale,

                "pack_pages": ms.pack_pages,
                "pack_records": ms.pack_records,
                "pack_pages_single": ms.pack_pages_single,
                "pack_avg_records_per_page": ms.avg_pack_records_per_page(),

                "bloom_updates_total": ms.bloom_updates_total,
                "bloom_update_bytes": ms.bloom_update_bytes,

                "lazy_compact_runs": ms.lazy_compact_runs,
                "lazy_compact_pages_written": ms.lazy_compact_pages_written
            }
        });

        // Печатаем pretty JSON
        println!("{}", serde_json::to_string_pretty(&status).unwrap());
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
        if mem_keydir_present {
            "present"
        } else {
            "absent"
        }
    );

    // Bloom side-car
    println!("Bloom:");
    if bloom_present {
        println!("  present          = true");
        println!("  buckets          = {}", bloom_buckets.unwrap_or(0));
        println!("  fresh            = {}", bloom_fresh.unwrap_or(false));
        if let Some(lsn) = bloom_last_lsn {
            println!("  last_lsn         = {}", lsn);
        }
        println!("  reason           = {}", bloom_reason);
        println!("  expected_buckets = {}", dir.bucket_count);
        println!("  expected_last_lsn= {}", db.pager.meta.last_lsn);
        if let Some(bpb) = bloom_bpb {
            println!("  bytes_per_bucket = {}", bpb);
        }
        if let Some(k) = bloom_k {
            println!("  k_hashes         = {}", k);
        }
    } else {
        println!("  present          = false");
        println!("  reason           = absent");
        println!("  expected_buckets = {}", dir.bucket_count);
        println!("  expected_last_lsn= {}", db.pager.meta.last_lsn);
    }
    // cache stats
    println!("  cache_cap        = {}", bloom_cache_cap);
    println!("  cache_entries    = {}", bloom_cache_entries);
    println!("  cache_hits       = {}", bloom_cache_hits);
    println!("  cache_misses     = {}", bloom_cache_misses);

    // Metrics snapshot (human)
    let ms = metrics::snapshot();
    println!("Metrics snapshot:");
    println!("  wal_appends_total       = {}", ms.wal_appends_total);
    println!("  wal_bytes_written       = {}", ms.wal_bytes_written);
    println!("  wal_fsync_calls         = {}", ms.wal_fsync_calls);
    println!(
        "  wal_avg_batch_pages     = {:.2}",
        ms.avg_wal_batch_pages()
    );
    println!("  wal_truncations         = {}", ms.wal_truncations);
    println!("  wal_pending_max_lsn     = {}", ms.wal_pending_max_lsn);
    println!("  wal_flushed_lsn         = {}", ms.wal_flushed_lsn);

    println!("  page_cache_hits         = {}", ms.page_cache_hits);
    println!("  page_cache_misses       = {}", ms.page_cache_misses);
    println!(
        "  page_cache_hit_ratio    = {:.2}%",
        ms.cache_hit_ratio() * 100.0
    );
    println!("  page_cache_len          = {}", page_cache_len());
    println!(
        "  page_cache_evictions    = {}",
        page_cache_evictions_total()
    );
    println!(
        "  page_cache_invalidations= {}",
        page_cache_invalidations_total()
    );

    println!("  keydir_hits             = {}", ms.keydir_hits);
    println!("  keydir_misses           = {}", ms.keydir_misses);
    println!(
        "  keydir_hit_ratio        = {:.2}%",
        ms.keydir_hit_ratio() * 100.0
    );

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
    println!("  pack_pages              = {}", ms.pack_pages);
    println!("  pack_records            = {}", ms.pack_records);
    println!("  pack_pages_single       = {}", ms.pack_pages_single);
    println!(
        "  pack_avg_records/page   = {:.2}",
        ms.avg_pack_records_per_page()
    );

    println!("  lazy_compact_runs       = {}", ms.lazy_compact_runs);
    println!(
        "  lazy_compact_pages_written = {}",
        ms.lazy_compact_pages_written
    );

    // Keep original stats printer (text or JSON via P1_DBSTATS_JSON)
    db.print_stats()?;
    Ok(())
}
