//! db/exists — быстрый presence‑check с keydir/bloom fast‑path.
//!
//! Семантика совпадает с get()/scan:
//! - Tombstone имеет приоритет (немедленный ответ false).
//! - TTL: expires_at_sec=0 — бессрочно; если now >= expires_at_sec — запись пропускается и ищем глубже.
//! - Tail‑wins: первый валидный от head считается присутствующим.
//!
//! Оптимизации:
//! - In‑memory keydir (если построен в RO): O(1) путь
//!     * keydir = NO_PAGE → сразу false
//!     * keydir = pid     → читаем одну страницу; если TTL истёк — fallback от next_pid
//! - Bloom fast‑path (если фильтр свеж и совместим по buckets):
//!     * test(bucket,key)==false → мгновенно false
//!     * test==true → лёгкий префетч головы (pager.prefetch_page(head)) для ускорения холодного чтения
//!       и затем обычный путь (или keydir‑hit, если включён)
//!
//! NEW: один буфер страницы на весь exists().
//! NEW: метрики keydir fast‑path: keydir_hits / keydir_misses.
//! NEW (perf): Bloom использует кэшированный sidecar в Db (bloom_ro), без повторных open() per call.

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};

use crate::bloom::BloomSidecar;
use crate::dir::NO_PAGE;
use crate::metrics::{
    record_bloom_negative, record_bloom_positive, record_bloom_skipped_stale, record_ttl_skipped,
    // NEW: keydir fast-path
    record_keydir_hit, record_keydir_miss,
};
use crate::page::{kv_header_read_v3, PAGE_MAGIC, PAGE_TYPE_KV_RH3};
use crate::page::kv::kv_find_record_by_key; // packed-aware поиск записи по ключу внутри страницы
use crate::util::now_secs;

use super::core::Db;

impl Db {
    /// Быстрый presence‑check с keydir/bloom fast‑path.
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        let bucket = self.dir.bucket_of_key(key, self.pager.meta.hash_kind);
        let ps = self.pager.meta.page_size as usize;
        let mut page_buf = vec![0u8; ps];
        let now = now_secs();

        // ---------- Fast path: in‑memory keydir ----------
        if let Some(pid) = self.mem_keydir_get(bucket, key) {
            // метрика keydir hit
            record_keydir_hit();

            // Tombstone (NO_PAGE) — приоритет
            if pid == NO_PAGE {
                return Ok(false);
            }
            if let Some(res) = self.exists_from_single_page_or_fallback_with_buf(key, pid, now, &mut page_buf)? {
                return Ok(res);
            }
            // Если вернулся None — fallback выполнит обычный путь ниже
        } else {
            // Если keydir построен, а записи нет — это промах fast-path
            if self.has_mem_keydir() {
                record_keydir_miss();
            }
        }

        // ---------- Bloom fast‑path ----------
        if let Some(sc_arc) = self.bloom_ro.as_ref() {
            // Deref Arc -> &BloomSidecar
            let sc: &BloomSidecar = sc_arc.as_ref();
            // Проверим совместимость (число бакетов) и свежесть (last_lsn)
            if sc.buckets() == self.dir.bucket_count && sc.is_fresh_for_db(self) {
                let maybe_present = sc.test(bucket, key)?;
                if maybe_present {
                    record_bloom_positive();
                    // NEW: префетч головы бакета (ускоряет холодный вариант)
                    let head = self.dir.head(bucket)?;
                    if head != NO_PAGE {
                        let _ = self.pager.prefetch_page(head);
                    }
                } else {
                    record_bloom_negative();
                    return Ok(false);
                }
            } else {
                record_bloom_skipped_stale();
            }
        } else {
            // Нет кэшированного sidecar (например, bloom.bin отсутствует) — скипаем
            record_bloom_skipped_stale();
        }

        // ---------- Обычный обход цепочки ----------
        let pid = self.dir.head(bucket)?;
        if pid == NO_PAGE {
            return Ok(false);
        }
        self.exists_scan_chain_from_with_buf(key, pid, now, &mut page_buf)
    }

    /// Проверка по одной странице pid с возможным fallback от next_pid.
    /// Возвращает:
    /// - Some(true/false) — удалось принять решение сразу (включая fallback)
    /// - None — если требуется продолжать обычный путь (редкий случай невалидного кадра/тип страницы)
    fn exists_from_single_page_or_fallback_with_buf(
        &self,
        key: &[u8],
        pid: u64,
        now: u32,
        page_buf: &mut [u8],
    ) -> Result<Option<bool>> {
        if pid == NO_PAGE {
            return Ok(Some(false));
        }

        let ps = self.pager.meta.page_size as usize;
        debug_assert_eq!(page_buf.len(), ps);

        self.pager.read_page(pid, page_buf)?;
        if &page_buf[0..4] != PAGE_MAGIC {
            // странно — дадим шанс обычному пути
            return Ok(None);
        }
        let ptype = LittleEndian::read_u16(&page_buf[6..8]);
        if ptype != PAGE_TYPE_KV_RH3 {
            return Ok(None);
        }

        let h = kv_header_read_v3(page_buf)?;

        // NEW: ищем запись по ключу в этой странице (packed-aware)
        if let Some((_k, _v, expires_at_sec, vflags)) = kv_find_record_by_key(page_buf, key) {
            // Tombstone — немедленно false
            if (vflags & 0x1) == 1 {
                return Ok(Some(false));
            }
            // TTL
            if expires_at_sec != 0 && now >= expires_at_sec {
                record_ttl_skipped();
                // Ищем глубже по цепочке
                return self.exists_scan_chain_from_with_buf(key, h.next_page_id, now, page_buf).map(Some);
            }
            // Валидная запись
            return Ok(Some(true));
        }

        // Запись для ключа не найдена на этой странице — пусть обычный путь проверит старшие страницы
        Ok(None)
    }

    /// Линейный обход цепочки от произвольного pid с TTL/tombstone семантикой.
    fn exists_scan_chain_from_with_buf(
        &self,
        key: &[u8],
        mut pid: u64,
        now: u32,
        page_buf: &mut [u8],
    ) -> Result<bool> {
        let ps = self.pager.meta.page_size as usize;
        debug_assert_eq!(page_buf.len(), ps);

        while pid != NO_PAGE {
            self.pager.read_page(pid, page_buf)?;
            if &page_buf[0..4] != PAGE_MAGIC {
                break;
            }
            let ptype = LittleEndian::read_u16(&page_buf[6..8]);
            if ptype != PAGE_TYPE_KV_RH3 {
                break;
            }

            let h = kv_header_read_v3(page_buf)?;
            // NEW: ищем запись на странице
            if let Some((_k, _v, expires_at_sec, vflags)) = kv_find_record_by_key(page_buf, key) {
                // Tombstone — отсутствует
                if (vflags & 0x1) == 1 {
                    return Ok(false);
                }
                // TTL
                if expires_at_sec != 0 && now >= expires_at_sec {
                    record_ttl_skipped();
                } else {
                    // Нашли валидную запись
                    return Ok(true);
                }
            }

            pid = h.next_page_id;
        }
        Ok(false)
    }
}