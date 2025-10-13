//! db/maintenance — сводные статистики БД и обслуживающие операции.
//!
//! Реализовано:
//! - Db::print_stats(): текстовый/JSON отчёт (ENV P1_DBSTATS_JSON=1|true|yes|on).
//! - Db::sweep_orphan_overflow(): writer‑операция — поиск и освобождение "сиротских" OVERFLOW3 страниц
//!   (добавляет page_id в free‑лист).
//! - NEW: Db::auto_maintenance(max_buckets, do_sweep): компактация ограниченного числа бакетов
//!   (tail‑wins без tombstone/expired) и, опционально, sweep сиротских OVERFLOW.
//! - NEW: Lazy compaction — Db::lazy_compact_bucket_if_needed(bucket) запускает компактацию
//!   бакета, если длина его цепочки ≥ порога (ENV P1_LAZY_COMPACT_THRESHOLD, по умолчанию 64).

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashSet;
use std::sync::OnceLock;

use crate::dir::NO_PAGE;
use crate::free::FreeList;
use crate::metrics;
use crate::page::{
    kv_header_read_v3, ovf_header_read_v3, PAGE_MAGIC, PAGE_TYPE_KV_RH3,
};
// packed-aware обход всех записей страницы
use crate::page::kv::kv_for_each_record;
// util: общий парсер OVERFLOW placeholder (TLV 0x01, len=16)
use crate::util::decode_ovf_placeholder_v3;

use super::core::Db;
// отчёты компактора
use super::compaction::{CompactBucketReport};

impl Db {
    /// Печать сводки по БД. JSON-вариант включается ENV P1_DBSTATS_JSON=1|true|yes|on.
    pub fn print_stats(&self) -> Result<()> {
        let json = std::env::var("P1_DBSTATS_JSON")
            .ok()
            .map(|s| s.to_ascii_lowercase())
            .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
            .unwrap_or(false);

        let ps = self.pager.meta.page_size;
        let pages = self.pager.meta.next_page_id;
        let buckets = self.dir.bucket_count;

        // used_buckets (best-effort)
        let used = self.dir.count_used_buckets()?;

        // chain length stats
        let (min_chain, avg_chain, max_chain) = self.chain_stats()?;

        // overflow pages and bytes
        let (ovf_pages, ovf_bytes) = self.overflow_stats()?;

        // free pages: читаем из free‑листа (best-effort)
        let free_pages = free_pages_count(&self.root).unwrap_or(0);

        // metrics snapshot
        let m = metrics::snapshot();
        let cache_total = m.page_cache_hits + m.page_cache_misses;
        let cache_hit_ratio = if cache_total > 0 {
            (m.page_cache_hits as f64) / (cache_total as f64)
        } else {
            0.0
        };

        if json {
            // JSON однострочник
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
                    \"sweep_orphan_runs\":{},\
                    \"snapshots_active\":{},\
                    \"snapshot_freeze_frames\":{},\
                    \"snapshot_freeze_bytes\":{},\
                    \"backup_pages_emitted\":{},\
                    \"backup_bytes_emitted\":{},\
                    \"restore_pages_written\":{},\
                    \"restore_bytes_written\":{},\
                    \"snapshot_fallback_scans\":{},\
                    \"ttl_skipped\":{}\
                }}",
                ps,
                pages,
                buckets,
                used,
                min_chain,
                avg_chain,
                max_chain,
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
                m.sweep_orphan_runs,
                m.snapshots_active,
                m.snapshot_freeze_frames,
                m.snapshot_freeze_bytes,
                m.backup_pages_emitted,
                m.backup_bytes_emitted,
                m.restore_pages_written,
                m.restore_bytes_written,
                m.snapshot_fallback_scans,
                m.ttl_skipped
            );
            return Ok(());
        }

        // Человеческий вывод
        println!("DB stats:");
        println!("  page_size        = {}", ps);
        println!("  pages_allocated  = {}", pages);
        println!("  buckets          = {}", buckets);
        println!("  used_buckets     = {}", used);
        if used > 0 {
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
        let m2 = metrics::snapshot();
        let cache_total2 = m2.page_cache_hits + m2.page_cache_misses;
        let cache_hit_ratio2 = if cache_total2 > 0 {
            (m2.page_cache_hits as f64) / (cache_total2 as f64)
        } else {
            0.0
        };
        println!("  wal_appends_total       = {}", m2.wal_appends_total);
        println!("  wal_bytes_written       = {}", m2.wal_bytes_written);
        println!("  wal_fsync_calls         = {}", m2.wal_fsync_calls);
        println!("  wal_avg_batch_pages     = {:.2}", m2.avg_wal_batch_pages());
        println!("  wal_truncations         = {}", m2.wal_truncations);
        println!("  page_cache_hits         = {}", m2.page_cache_hits);
        println!("  page_cache_misses       = {}", m2.page_cache_misses);
        println!("  page_cache_hit_ratio    = {:.2}%", cache_hit_ratio2 * 100.0);
        println!("  rh_page_compactions     = {}", m2.rh_page_compactions);
        println!("  overflow_chains_created = {}", m2.overflow_chains_created);
        println!("  overflow_chains_freed   = {}", m2.overflow_chains_freed);
        println!("  sweep_orphan_runs       = {}", m2.sweep_orphan_runs);
        println!("  snapshots_active        = {}", m2.snapshots_active);
        println!("  snapshot_freeze_frames  = {}", m2.snapshot_freeze_frames);
        println!("  snapshot_freeze_bytes   = {}", m2.snapshot_freeze_bytes);
        println!("  backup_pages_emitted    = {}", m2.backup_pages_emitted);
        println!("  backup_bytes_emitted    = {}", m2.backup_bytes_emitted);
        println!("  restore_pages_written   = {}", m2.restore_pages_written);
        println!("  restore_bytes_written   = {}", m2.restore_bytes_written);
        println!("  snapshot_fallback_scans = {}", m2.snapshot_fallback_scans);
        println!("  ttl_skipped             = {}", m2.ttl_skipped);

        Ok(())
    }

    /// Writer‑операция: собрать и освободить "сиротские" OVERFLOW3 страницы.
    /// Возвращает число освобождённых страниц.
    pub fn sweep_orphan_overflow(&mut self) -> Result<usize> {
        if self.readonly {
            return Err(anyhow!("sweep_orphan_overflow: Db is read-only"));
        }
        metrics::record_sweep_orphan_run();

        let ps = self.pager.meta.page_size as usize;
        let total_pages = self.pager.meta.next_page_id;

        // 1) Сбор "помеченных" overflow страниц, достижимых из KV цепочек (по placeholder’ам).
        let mut marked: HashSet<u64> = HashSet::new();

        for b in 0..self.dir.bucket_count {
            let mut pid = self.dir.head(b)?;
            while pid != NO_PAGE {
                let mut page = vec![0u8; ps];
                if self.pager.read_page(pid, &mut page).is_err() {
                    break;
                }
                if &page[0..4] != PAGE_MAGIC {
                    break;
                }
                // KV_RH3?
                let ptype = LittleEndian::read_u16(&page[6..8]);
                if ptype != PAGE_TYPE_KV_RH3 {
                    break;
                }
                let h = kv_header_read_v3(&page)?;

                // Обойдём все записи страницы (packed-aware)
                kv_for_each_record(&page, |_, v, _exp, vflags| {
                    // tombstone не содержит значения — игнор
                    if (vflags & 0x1) != 0 {
                        return;
                    }
                    if let Some((_total_len, head_pid)) = decode_ovf_placeholder_v3(v) {
                        // пройти по всей цепочке и пометить
                        let mut cur = head_pid;
                        let mut guard = 0usize;
                        while cur != NO_PAGE {
                            guard += 1;
                            if guard > 1_000_000 {
                                break;
                            }
                            if !marked.insert(cur) {
                                // уже помечен — просто перейдём дальше
                                cur = read_ovf_next_pid_silent(self, cur, ps).unwrap_or(NO_PAGE);
                                continue;
                            }
                            cur = read_ovf_next_pid_silent(self, cur, ps).unwrap_or(NO_PAGE);
                        }
                    }
                });

                pid = h.next_page_id;
            }
        }

        // 2) Обход всех страниц: если это OVERFLOW3 и не помечена — освобождаем.
        let mut freed = 0usize;

        for pid in 0..total_pages {
            if marked.contains(&pid) {
                continue;
            }
            let mut buf = vec![0u8; ps];
            if self.pager.read_page(pid, &mut buf).is_ok() {
                if let Ok(h) = ovf_header_read_v3(&buf) {
                    let _ = h; // факт, что это OVF‑страница
                    self.pager.free_page(pid)?;
                    freed += 1;
                }
            }
        }

        Ok(freed)
    }

    // -------------------- helpers (методы Db) --------------------

    /// Статистика по длинам цепочек (head→tail) среди непустых bucket’ов.
    fn chain_stats(&self) -> Result<(u64, f64, u64)> {
        let ps = self.pager.meta.page_size as usize;
        let mut min_chain = u64::MAX;
        let mut max_chain = 0u64;
        let mut sum_chain = 0u64;
        let mut non_empty = 0u64;

        for b in 0..self.dir.bucket_count {
            let mut len = 0u64;
            let mut pid = self.dir.head(b)?;
            while pid != NO_PAGE {
                len += 1;
                let mut buf = vec![0u8; ps];
                if self.pager.read_page(pid, &mut buf).is_err() {
                    break;
                }
                if &buf[0..4] != PAGE_MAGIC {
                    break;
                }
                let h = match kv_header_read_v3(&buf) {
                    Ok(h) => h,
                    Err(_) => break,
                };
                pid = h.next_page_id;
            }
            if len > 0 {
                non_empty += 1;
                min_chain = min_chain.min(len);
                max_chain = max_chain.max(len);
                sum_chain += len;
            }
        }

        let avg = if non_empty > 0 {
            sum_chain as f64 / non_empty as f64
        } else {
            0.0
        };
        if non_empty == 0 {
            min_chain = 0;
        }
        Ok((min_chain, avg, max_chain))
    }

    /// Подсчёт overflow страниц и суммарных "данных" (chunk_len).
    fn overflow_stats(&self) -> Result<(u64, u64)> {
        let ps = self.pager.meta.page_size as usize;
        let total_pages = self.pager.meta.next_page_id;

        let mut cnt = 0u64;
        let mut bytes = 0u64;

        for pid in 0..total_pages {
            let mut buf = vec![0u8; ps];
            if self.pager.read_page(pid, &mut buf).is_ok() {
                if let Ok(h) = ovf_header_read_v3(&buf) {
                    cnt += 1;
                    bytes += h.chunk_len as u64;
                }
            }
        }
        Ok((cnt, bytes))
    }

    /// NEW: длина цепочки одного бакета (head → tail).
    fn chain_len_for_bucket(&self, bucket: u32) -> Result<u64> {
        let ps = self.pager.meta.page_size as usize;
        let mut len = 0u64;
        let mut pid = self.dir.head(bucket)?;
        while pid != NO_PAGE {
            len += 1;
            let mut buf = vec![0u8; ps];
            if self.pager.read_page(pid, &mut buf).is_err() {
                break;
            }
            if &buf[0..4] != PAGE_MAGIC {
                break;
            }
            let h = match kv_header_read_v3(&buf) {
                Ok(h) => h,
                Err(_) => break,
            };
            pid = h.next_page_id;
        }
        Ok(len)
    }

    /// NEW: ленивое решение — если цепочка бакета длиннее порога, запустить компактацию.
    pub fn lazy_compact_bucket_if_needed(&mut self, bucket: u32) -> Result<Option<CompactBucketReport>> {
        if self.readonly {
            return Err(anyhow!("lazy_compact_bucket_if_needed: Db is read-only"));
        }
        let threshold = lazy_compact_threshold();
        if threshold == 0 {
            return Ok(None);
        }
        let len = self.chain_len_for_bucket(bucket)?;
        if len >= threshold {
            let rep = self.compact_bucket(bucket)?;
            // метрика: один запуск lazy-compact, учитываем страницы
            metrics::record_lazy_compact_run(rep.pages_written);
            eprintln!(
                "[INFO] lazy-compact: bucket {} (len={} ≥ threshold={}) -> pages_written={}",
                bucket, len, threshold, rep.pages_written
            );
            return Ok(Some(rep));
        }
        Ok(None)
    }
}

// -------------------- helpers (статические) --------------------

/// Прочитать next_page_id из OVERFLOW3 страницы pid, игнорируя ошибки (NO_PAGE при ошибке).
fn read_ovf_next_pid_silent(db: &Db, pid: u64, ps: usize) -> Result<u64> {
    let mut page = vec![0u8; ps];
    db.pager.read_page(pid, &mut page)?;
    let h = ovf_header_read_v3(&page)?;
    Ok(h.next_page_id)
}

/// Подсчёт свободных страниц (через free‑лист).
fn free_pages_count(root: &std::path::Path) -> Result<u64> {
    let fl = match FreeList::open(root) {
        Ok(fl) => fl,
        Err(_) => return Ok(0),
    };
    fl.count()
}

// -------------------- NEW: авто‑обслуживание --------------------

/// Сводка авто‑обслуживания: компактация части бакетов и sweep сиротских OVERFLOW.
#[derive(Debug, Clone)]
pub struct AutoMaintSummary {
    /// Сколько непустых бакетов было отсканировано (head != NO_PAGE).
    pub buckets_scanned: u32,
    /// Сколько бакетов было скомпактировано.
    pub buckets_compacted: u32,
    /// Сколько страниц было записано компактором (суммарно).
    pub pages_written_sum: u64,
    /// Сколько OVERFLOW‑страниц освобождено sweep’ом (если do_sweep=true).
    pub overflow_pages_freed: usize,
}

impl Db {
    /// Автоматическое обслуживание: компактация первых `max_buckets` непустых бакетов
    /// (в порядке их индекса) и, опционально, sweep сиротских OVERFLOW‑страниц.
    ///
    /// Возвращает сводку по выполненной работе.
    pub fn auto_maintenance(&mut self, max_buckets: u32, do_sweep: bool) -> Result<AutoMaintSummary> {
        if self.readonly {
            return Err(anyhow!("auto_maintenance: Db is read-only"));
        }

        let mut scanned = 0u32;
        let mut compacted = 0u32;
        let mut pages_written_sum = 0u64;

        for b in 0..self.dir.bucket_count {
            if scanned >= max_buckets {
                break;
            }
            let head = self.dir.head(b)?;
            if head == NO_PAGE {
                continue;
            }
            // считаем только непустые бакеты
            scanned += 1;

            let rep = self.compact_bucket(b)?;
            if rep.pages_written > 0 {
                compacted += 1;
                pages_written_sum += rep.pages_written;
            }
        }

        let overflow_pages_freed = if do_sweep {
            self.sweep_orphan_overflow()?
        } else {
            0
        };

        Ok(AutoMaintSummary {
            buckets_scanned: scanned,
            buckets_compacted: compacted,
            pages_written_sum,
            overflow_pages_freed,
        })
    }
}

// ---------- ENV helpers ----------

fn lazy_compact_threshold() -> u64 {
    static THR: OnceLock<u64> = OnceLock::new();
    *THR.get_or_init(|| {
        std::env::var("P1_LAZY_COMPACT_THRESHOLD")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(64)
    })
}