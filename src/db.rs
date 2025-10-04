use crate::consts::{NO_PAGE, PAGE_HDR_V2_SIZE};
use crate::dir::Directory;
use crate::lock::{acquire_exclusive_lock, LockGuard}; // locking
use crate::meta::set_clean_shutdown; // clean shutdown flag
use crate::page_rh::{
    rh_compact_inplace, rh_header_read, rh_header_write, rh_kv_delete_inplace, rh_kv_insert,
    rh_kv_lookup, rh_page_init, rh_page_is_kv, rh_should_compact, RH_SLOT_SIZE,
};
use crate::pager::Pager;
use crate::wal::{wal_replay_if_any, Wal, WalGroupCfg};
use anyhow::{anyhow, Result};

pub struct Db {
    pub root: std::path::PathBuf,
    pub pager: Pager,
    pub dir: Directory,
    // Держим эксклюзивную блокировку на время жизни Db.
    // Это защищает от одновременной записи из нескольких процессов.
    _lock: LockGuard, // переименовано для подавления предупреждения о неиспользовании
}

impl Db {
    /// Открыть БД с эксклюзивной блокировкой (single-writer).
    /// Выполняет WAL replay при необходимости и помечает запуск как "нечистый"
    /// до момента закрытия (Drop).
    /// Включает group-commit для WAL, если задано окружение P1_WAL_COALESCE_MS (мс, по умолчанию 3).
    pub fn open(root: &std::path::Path) -> Result<Self> {
        // 1) Блокируемся эксклюзивно
        let lock = acquire_exclusive_lock(root)?;

        // 1.1) Настроим group-commit для WAL (best-effort).
        let coalesce_ms = std::env::var("P1_WAL_COALESCE_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(3);
        Wal::set_group_config(root, WalGroupCfg { coalesce_ms })?;

        // 2) Реплей WAL (сам wal.rs решает, нужен ли реплей, исходя из clean_shutdown)
        wal_replay_if_any(root)?;

        // 3) Открываем подсистемы
        let pager = Pager::open(root)?;
        let dir = Directory::open(root)?;

        // 4) Помечаем запуск как "нечистый" — если процесс умрёт,
        //    при следующем старте будет реплей.
        set_clean_shutdown(root, false)?;

        Ok(Self {
            root: root.to_path_buf(),
            pager,
            dir,
            _lock: lock,
        })
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        // Проверим, что запись вообще может поместиться в пустую v2 страницу.
        // Берём минимальный один слот таблицы.
        let rec_sz = 4 + key.len() + val.len();
        let ps = self.pager.meta.page_size as usize;
        if rec_sz + RH_SLOT_SIZE + PAGE_HDR_V2_SIZE > ps {
            return Err(anyhow!(
                "record too large: key={} value={} for page_size {}",
                key.len(),
                val.len(),
                ps
            ));
        }

        let bucket = self.dir.bucket_of_key(key);
        let mut head = self.dir.head(bucket)?;
        if head == NO_PAGE {
            // создаём первую страницу в бакете (всегда v2), предпочтительно из free-list
            let new_pid = self.pager.allocate_one_page()?;
            let mut buf = vec![0u8; ps];
            rh_page_init(&mut buf, new_pid)?;
            let ok = rh_kv_insert(&mut buf, self.dir.hash_kind, key, val)?;
            if !ok {
                return Err(anyhow!("unexpected: empty v2 page cannot fit record"));
            }
            self.pager.commit_page(new_pid, &mut buf)?;
            self.dir.set_head(bucket, new_pid)?;
            return Ok(());
        }

        // Ищем место в цепочке v2-страниц, по пути вычищаем пустые страницы
        let mut prev: u64 = NO_PAGE;
        let mut pid: u64 = head;
        let mut tail_pid: u64 = NO_PAGE;

        loop {
            let mut buf = vec![0u8; ps];
            self.pager.read_page(pid, &mut buf)?;
            if !rh_page_is_kv(&buf) {
                return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
            }

            // 1) Пытаемся вставить сразу
            if rh_kv_insert(&mut buf, self.dir.hash_kind, key, val)? {
                self.pager.commit_page(pid, &mut buf)?;
                return Ok(());
            }

            // 2) Оппортунистическая компакция по порогам и повторная попытка
            if rh_should_compact(&buf)? {
                rh_compact_inplace(&mut buf, self.dir.hash_kind)?;
                if rh_kv_insert(&mut buf, self.dir.hash_kind, key, val)? {
                    self.pager.commit_page(pid, &mut buf)?;
                    return Ok(());
                }
            }

            // 3) Проверим, не пустая ли страница — и если пустая, вырежем её из цепочки
            let h = rh_header_read(&buf)?;
            if h.used_slots == 0 {
                // unlink pid -> h.next_page_id
                let next = h.next_page_id;
                if prev == NO_PAGE {
                    // срезаем head
                    self.dir.set_head(bucket, next)?;
                    head = next;
                } else {
                    // обновим next у prev и закоммитим
                    let mut pbuf = vec![0u8; ps];
                    self.pager.read_page(prev, &mut pbuf)?;
                    let mut ph = rh_header_read(&pbuf)?;
                    ph.next_page_id = next;
                    rh_header_write(&mut pbuf, &ph)?;
                    self.pager.commit_page(prev, &mut pbuf)?;
                }
                // страницу — в free-list (после unlink)
                let _ = self.pager.free_page(pid);
                // продолжаем с next, prev остаётся прежним
                if next == NO_PAGE {
                    tail_pid = prev; // цепочка закончилась на prev
                    break;
                }
                pid = next;
                continue;
            }

            // 4) Переходим дальше по цепочке
            if h.next_page_id == NO_PAGE {
                tail_pid = pid;
                break;
            }
            prev = pid;
            pid = h.next_page_id;
        }

        // Добавляем новую v2 страницу в конец цепочки (предпочтительно из free-list)
        let new_pid = self.pager.allocate_one_page()?;
        let mut newb = vec![0u8; ps];
        rh_page_init(&mut newb, new_pid)?;
        let ok = rh_kv_insert(&mut newb, self.dir.hash_kind, key, val)?;
        if !ok {
            return Err(anyhow!("unexpected: empty v2 page cannot fit record"));
        }
        self.pager.commit_page(new_pid, &mut newb)?;

        // Обновим next у хвоста (tail_pid может быть NO_PAGE, если цепочка полностью очистилась)
        if tail_pid == NO_PAGE {
            // цепочка стала пустой — новый pid становится head
            self.dir.set_head(bucket, new_pid)?;
        } else {
            let mut tailb = vec![0u8; ps];
            self.pager.read_page(tail_pid, &mut tailb)?;
            let mut th = rh_header_read(&tailb)?;
            th.next_page_id = new_pid;
            rh_header_write(&mut tailb, &th)?;
            self.pager.commit_page(tail_pid, &mut tailb)?;
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let bucket = self.dir.bucket_of_key(key);
        let mut pid = self.dir.head(bucket)?;
        if pid == NO_PAGE {
            return Ok(None);
        }
        let ps = self.pager.meta.page_size as usize;
        let mut found: Option<Vec<u8>> = None;
        while pid != NO_PAGE {
            let mut buf = vec![0u8; ps];
            self.pager.read_page(pid, &mut buf)?;
            if !rh_page_is_kv(&buf) {
                return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
            }
            if let Some(v) = rh_kv_lookup(&buf, self.dir.hash_kind, key)? {
                found = Some(v);
            }
            let h = rh_header_read(&buf)?;
            pid = h.next_page_id;
        }
        Ok(found)
    }

    pub fn del(&mut self, key: &[u8]) -> Result<bool> {
        let bucket = self.dir.bucket_of_key(key);
        let mut head = self.dir.head(bucket)?;
        if head == NO_PAGE {
            return Ok(false);
        }

        let ps = self.pager.meta.page_size as usize;
        let mut existed_any = false;

        // prev -> pid -> next обход с очисткой пустых страниц (unlink + free)
        let mut prev: u64 = NO_PAGE;
        let mut pid: u64 = head;

        while pid != NO_PAGE {
            // загрузим страницу
            let mut buf = vec![0u8; ps];
            self.pager.read_page(pid, &mut buf)?;
            if !rh_page_is_kv(&buf) {
                return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
            }

            // попытка удаления
            let mut modified = false;
            if rh_kv_delete_inplace(&mut buf, self.dir.hash_kind, key)? {
                existed_any = true;
                modified = true;
            }

            if modified {
                // зафиксируем изменения страницы (LSN, CRC, WAL)
                self.pager.commit_page(pid, &mut buf)?;
            }

            // проверим, не стала ли страница пустой (или уже была пустой)
            let h = rh_header_read(&buf)?;
            let next = h.next_page_id;

            if h.used_slots == 0 {
                // unlink этой страницы из цепочки
                if prev == NO_PAGE {
                    // удаляем head
                    self.dir.set_head(bucket, next)?;
                    head = next;
                } else {
                    // обновим next у prev и закоммитим
                    let mut pbuf = vec![0u8; ps];
                    self.pager.read_page(prev, &mut pbuf)?;
                    let mut ph = rh_header_read(&pbuf)?;
                    ph.next_page_id = next;
                    rh_header_write(&mut pbuf, &ph)?;
                    self.pager.commit_page(prev, &mut pbuf)?;
                }

                // отправим текущую страницу в free-list (после unlink)
                let _ = self.pager.free_page(pid);

                // не двигаем prev, переходим к next
                pid = next;
                continue;
            }

            // страница не пустая — двигаем prev и pid
            prev = pid;
            pid = next;
        }

        Ok(existed_any)
    }

    pub fn print_stats(&self) -> Result<()> {
        let ps = self.pager.meta.page_size;
        let pages = self.pager.meta.next_page_id;
        let buckets = self.dir.bucket_count;
        let used = self.dir.count_used_buckets()?;

        let mut min_chain = u64::MAX;
        let mut max_chain = 0u64;
        let mut sum_chain = 0u64;

        for b in 0..buckets {
            let mut len = 0u64;
            let mut pid = self.dir.head(b)?;
            while pid != NO_PAGE {
                len += 1;
                let mut buf = vec![0u8; ps as usize];
                self.pager.read_page(pid, &mut buf)?;
                if !rh_page_is_kv(&buf) {
                    return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
                }
                let h = rh_header_read(&buf)?;
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

        // ----- Metrics snapshot -----
        let m = crate::metrics::snapshot();
        let cache_total = m.page_cache_hits + m.page_cache_misses;
        let cache_hit_ratio = if cache_total > 0 {
            (m.page_cache_hits as f64) / (cache_total as f64)
        } else {
            0.0
        };

        println!("Metrics:");
        println!("  wal_appends_total       = {}", m.wal_appends_total);
        println!("  wal_bytes_written       = {}", m.wal_bytes_written);
        println!("  wal_fsync_calls         = {}", m.wal_fsync_calls);
        println!(
            "  wal_avg_batch_pages     = {:.2}",
            m.avg_wal_batch_pages()
        );
        println!("  wal_truncations         = {}", m.wal_truncations);
        println!("  page_cache_hits         = {}", m.page_cache_hits);
        println!("  page_cache_misses       = {}", m.page_cache_misses);
        println!("  page_cache_hit_ratio    = {:.2}%", cache_hit_ratio * 100.0);
        println!("  rh_page_compactions     = {}", m.rh_page_compactions);

        Ok(())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // По возможности пометим «чистое завершение»
        let _ = set_clean_shutdown(&self.root, true);
        // LockGuard освободит файловую блокировку автоматически.
    }
}