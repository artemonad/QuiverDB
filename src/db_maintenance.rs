use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use crate::Db;
use crate::consts::{FREE_FILE, FREE_HDR_SIZE, FREE_MAGIC, NO_PAGE};
use crate::free::FreeList;
use crate::page_ovf::{ovf_header_read};
use crate::page_rh::{rh_header_read, rh_page_is_kv};
use crate::util::display_text;

/// Прочитать множество page_id из free‑листа (read‑only).
pub fn read_free_set(db: &Db) -> Result<HashSet<u64>> {
    let path = db.root.join(FREE_FILE);
    let mut set = HashSet::new();
    if !path.exists() {
        return Ok(set);
    }
    let mut f = OpenOptions::new().read(true).open(&path)?;
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    if &magic != FREE_MAGIC {
        return Ok(set);
    }
    let mut hdr = vec![0u8; FREE_HDR_SIZE - 8];
    f.read_exact(&mut hdr)?;
    let len = f.metadata()?.len();
    let mut pos = FREE_HDR_SIZE as u64;
    while pos + 8 <= len {
        let mut buf8 = [0u8; 8];
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut buf8)?;
        let pid = LittleEndian::read_u64(&buf8);
        set.insert(pid);
        pos += 8;
    }
    Ok(set)
}

/// Сбор сиротских overflow‑страниц и их отправка в free‑лист (writer‑only).
/// ВНИМАНИЕ: не проверяет db.readonly (полагается на вызывающего).
pub fn sweep_orphan_overflow_writer(db: &Db) -> Result<()> {
    let ps = db.pager.meta.page_size as usize;
    let pages_alloc = db.pager.meta.next_page_id;
    let free_set = read_free_set(db)?;

    // метрика: запуск sweep
    crate::metrics::record_sweep_orphan_run();

    // Сбор голов overflow‑цепей из реальных цепочек каталога.
    let mut heads: Vec<u64> = Vec::new();
    for b in 0..db.dir.bucket_count {
        let mut pid = db.dir.head(b)?;
        while pid != NO_PAGE {
            let mut buf = vec![0u8; ps];
            if db.pager.read_page(pid, &mut buf).is_err() {
                break;
            }
            if !rh_page_is_kv(&buf) {
                break;
            }
            if let Ok(items) = crate::page_rh::rh_kv_list(&buf) {
                for (_k, v) in items {
                    if v.len() == 18 && v[0] == 0xFF {
                        let head_pid = LittleEndian::read_u64(&v[10..18]);
                        heads.push(head_pid);
                    }
                }
            }
            let h = rh_header_read(&buf)?;
            pid = h.next_page_id;
        }
    }

    if heads.is_empty() {
        return Ok(());
    }

    // Mark
    let mut marked: HashSet<u64> = HashSet::new();
    for head_pid in heads {
        let mut cur = head_pid;
        while cur != NO_PAGE {
            if marked.contains(&cur) {
                break;
            }
            let mut buf = vec![0u8; ps];
            if db.pager.read_page(cur, &mut buf).is_err() {
                break;
            }
            if let Ok(h) = ovf_header_read(&buf) {
                marked.insert(cur);
                cur = h.next_page_id;
            } else {
                break;
            }
        }
    }

    // Sweep
    for pid in 0..pages_alloc {
        if free_set.contains(&pid) {
            continue;
        }
        let mut buf = vec![0u8; ps];
        if db.pager.read_page(pid, &mut buf).is_ok() {
            if ovf_header_read(&buf).is_ok() && !marked.contains(&pid) {
                db.pager.free_page(pid)?;
            }
        }
    }
    Ok(())
}

/// Вывести статистику БД (включая метрики); поддерживает ENV P1_DBSTATS_JSON.
pub fn print_stats(db: &Db) -> Result<()> {
    // JSON toggle via ENV (без изменений CLI)
    let json = std::env::var("P1_DBSTATS_JSON")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false);

    let ps = db.pager.meta.page_size;
    let pages = db.pager.meta.next_page_id;
    let buckets = db.dir.bucket_count;
    let used = db.dir.count_used_buckets()?;

    let mut min_chain = u64::MAX;
    let mut max_chain = 0u64;
    let mut sum_chain = 0u64;

    for b in 0..buckets {
        let mut len = 0u64;
        let mut pid = db.dir.head(b)?;
        while pid != NO_PAGE {
            len += 1;
            let mut buf = vec![0u8; ps as usize];
            db.pager.read_page(pid, &mut buf)?;
            if !crate::page_rh::rh_page_is_kv(&buf) {
                return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
            }
            let h = crate::page_rh::rh_header_read(&buf)?;
            pid = h.next_page_id;
        }
        if len > 0 {
            min_chain = min_chain.min(len);
            max_chain = max_chain.max(len);
            sum_chain += len;
        }
    }
    let non_empty = used as u64;
    let avg_chain = if non_empty > 0 {
        sum_chain as f64 / (non_empty as f64)
    } else {
        0.0
    };

    let free_set = read_free_set(db)?;
    let mut ovf_pages = 0u64;
    let mut ovf_bytes = 0u64;
    for pid in 0..pages {
        if free_set.contains(&pid) {
            continue;
        }
        let mut buf = vec![0u8; ps as usize];
        if db.pager.read_page(pid, &mut buf).is_ok() {
            if let Ok(h) = ovf_header_read(&buf) {
                ovf_pages += 1;
                ovf_bytes += h.chunk_len as u64;
            }
        }
    }
    let free_pages = match FreeList::open(&db.root) {
        Ok(fl) => fl.count().unwrap_or(0),
        Err(_) => 0,
    };

    // ----- Metrics snapshot -----
    let m = crate::metrics::snapshot();
    let cache_total = m.page_cache_hits + m.page_cache_misses;
    let cache_hit_ratio = if cache_total > 0 {
        (m.page_cache_hits as f64) / (cache_total as f64)
    } else {
        0.0
    };

    if json {
        let chain_min = if non_empty > 0 { min_chain } else { 0 };
        let chain_max = if non_empty > 0 { max_chain } else { 0 };
        println!(
            "{{\
                \"page_size\":{},\
                \"pages_allocated\":{},\
                \"buckets\":{},\
                \"used_buckets\":{},\
                \"chain_len_min\":{},\
                \"chain_len_avg\":{:.2},\
                \"chain_len_max\":{},\
                \"overflow_pages\":{},\
                \"overflow_bytes\":{},\
                \"free_pages\":{},\
                \"wal_appends_total\":{},\
                \"wal_bytes_written\":{},\
                \"wal_fsync_calls\":{},\
                \"wal_avg_batch_pages\":{:.2},\
                \"wal_truncations\":{},\
                \"page_cache_hits\":{},\
                \"page_cache_misses\":{},\
                \"page_cache_hit_ratio\":{:.2},\
                \"rh_page_compactions\":{},\
                \"overflow_chains_created\":{},\
                \"overflow_chains_freed\":{},\
                \"sweep_orphan_runs\":{}\
            }}",
            ps,
            pages,
            buckets,
            used,
            chain_min,
            avg_chain,
            chain_max,
            ovf_pages,
            ovf_bytes,
            free_pages,
            m.wal_appends_total,
            m.wal_bytes_written,
            m.wal_fsync_calls,
            m.avg_wal_batch_pages(),
            m.wal_truncations,
            m.page_cache_hits,
            m.page_cache_misses,
            cache_hit_ratio * 100.0,
            m.rh_page_compactions,
            m.overflow_chains_created,
            m.overflow_chains_freed,
            m.sweep_orphan_runs
        );
        return Ok(());
    }

    println!("DB stats:");
    println!("  page_size        = {}", ps);
    println!("  pages_allocated  = {}", pages);
    println!("  buckets          = {}", buckets);
    println!("  used_buckets     = {}", used);
    if non_empty > 0 {
        println!(
            "  chain_len min/avg/max = {}/{:.2}/{}",
            min_chain, avg_chain, max_chain
        );
    } else {
        println!("  chain_len min/avg/max = n/a");
    }
    println!("  overflow_pages    = {}", ovf_pages);
    println!("  overflow_bytes    = {}", ovf_bytes);
    println!("  free_pages        = {}", free_pages);

    println!("Metrics:");
    println!("  wal_appends_total       = {}", m.wal_appends_total);
    println!("  wal_bytes_written       = {}", m.wal_bytes_written);
    println!("  wal_fsync_calls         = {}", m.wal_fsync_calls);
    println!("  wal_avg_batch_pages     = {:.2}", m.avg_wal_batch_pages());
    println!("  wal_truncations         = {}", m.wal_truncations);
    println!("  page_cache_hits         = {}", m.page_cache_hits);
    println!("  page_cache_misses       = {}", m.page_cache_misses);
    println!("  page_cache_hit_ratio    = {:.2}%", cache_hit_ratio * 100.0);
    println!("  rh_page_compactions     = {}", m.rh_page_compactions);
    println!("  overflow_chains_created = {}", m.overflow_chains_created);
    println!("  overflow_chains_freed   = {}", m.overflow_chains_freed);
    println!("  sweep_orphan_runs       = {}", m.sweep_orphan_runs);

    Ok(())
}