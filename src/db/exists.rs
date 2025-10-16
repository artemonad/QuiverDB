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
//!     * keydir = (pid, off) → читаем ровно нужную запись на странице;
//!       если TTL истёк — fallback от next_pid
//! - Bloom fast‑path (если фильтр свеж и совместим по buckets):
//!     * test(bucket,key)==false → мгновенно false
//!     * test==true → лёгкий префетч головы (pager.prefetch_page(head)) для ускорения холодного чтения
//!       и затем обычный путь (или keydir‑hit, если включён)
//!
//! Изменения (ускорение):
//! - Exists теперь использует локацию (pid, off) из in‑memory keydir и
//!   безопасный ридер kv_read_record_at_checked для прямой проверки, избегая
//!   внутристраничного поиска. При любом несоответствии — корректный fallback.

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};

use crate::bloom::BloomSidecar;
use crate::dir::NO_PAGE;
use crate::metrics::{
    record_bloom_negative, record_bloom_positive, record_bloom_skipped_stale, record_ttl_skipped,
    record_keydir_hit, record_keydir_miss,
};
use crate::page::{
    kv_header_read_v3, PAGE_MAGIC, PAGE_TYPE_KV_RH3, TRAILER_LEN,
};
use crate::page::kv::{kv_for_each_record, kv_read_record_at_checked};
use crate::page::common::KV_SLOT_SIZE;
use crate::util::now_secs;

use super::core::{Db, MemKeyLoc};

impl Db {
    /// Быстрый presence‑check с keydir/bloom fast‑path.
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        let bucket = self.dir.bucket_of_key(key, self.pager.meta.hash_kind);
        let ps = self.pager.meta.page_size as usize;
        let mut page_buf = vec![0u8; ps];
        let now = now_secs();

        // ---------- Fast path: in‑memory keydir с оффсетом ----------
        if let Some(loc) = self.mem_keydir_get_loc(bucket, key) {
            record_keydir_hit();

            if loc.pid == NO_PAGE {
                return Ok(false);
            }
            if let Some(res) = self.exists_from_pid_off_or_fallback_with_buf(key, loc, now, &mut page_buf)? {
                return Ok(res);
            }
        } else {
            if self.has_mem_keydir() {
                record_keydir_miss();
            }
        }

        // ---------- Bloom fast‑path ----------
        if let Some(sc_arc) = self.bloom_ro.as_ref() {
            let sc: &BloomSidecar = sc_arc.as_ref();
            if sc.buckets() == self.dir.bucket_count && sc.is_fresh_for_db(self) {
                let maybe_present = sc.test(bucket, key)?;
                if maybe_present {
                    record_bloom_positive();
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
            record_bloom_skipped_stale();
        }

        // ---------- Обычный обход цепочки ----------
        let pid = self.dir.head(bucket)?;
        if pid == NO_PAGE {
            return Ok(false);
        }
        self.exists_scan_chain_from_with_buf(key, pid, now, &mut page_buf)
    }

    /// Быстрый путь по точной локации (pid, off). Возвращает:
    /// - Some(true/false) — удалось принять решение (включая fallback),
    /// - None — если требуется продолжать обычный путь.
    fn exists_from_pid_off_or_fallback_with_buf(
        &self,
        key: &[u8],
        loc: MemKeyLoc,
        now: u32,
        page_buf: &mut [u8],
    ) -> Result<Option<bool>> {
        let ps = self.pager.meta.page_size as usize;
        debug_assert_eq!(page_buf.len(), ps);

        if loc.pid == NO_PAGE {
            return Ok(Some(false));
        }

        self.pager.read_page(loc.pid, page_buf)?;
        if &page_buf[0..4] != PAGE_MAGIC {
            // странная страница — пусть обычный путь разберётся (от головы)
            return Ok(None);
        }
        let ptype = LittleEndian::read_u16(&page_buf[6..8]);
        if ptype != PAGE_TYPE_KV_RH3 {
            return Ok(None);
        }

        let h = kv_header_read_v3(page_buf)?;
        let next = h.next_page_id;
        let data_end = data_end_for_header(&h, ps);

        // Прямая проверка по off
        let off = loc.off as usize;
        if let Some((k, _v, expires_at_sec, vflags)) = kv_read_record_at_checked(page_buf, off, data_end) {
            if k != key {
                // рассогласование (редко) — пусть обычный путь решает
                return Ok(None);
            }
            if (vflags & 0x1) == 1 {
                // tombstone — немедленно false
                return Ok(Some(false));
            }
            if expires_at_sec != 0 && now >= expires_at_sec {
                record_ttl_skipped();
                // Протухло: возможно есть более старая валидная версия — fallback от next_pid
                return self.exists_scan_chain_from_with_buf(key, next, now, page_buf).map(Some);
            }
            // Валидная запись — присутствует
            return Ok(Some(true));
        }

        // Если по off не получилось — оставим решение обычному пути
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
            let next = h.next_page_id;

            match self.page_decide_exists_for_key(page_buf, key, now) {
                DecideExists::Tombstone => return Ok(false),
                DecideExists::Present => return Ok(true),
                DecideExists::Continue => {
                    pid = next;
                }
            }
        }
        Ok(false)
    }

    /// Решение по одной странице для exists(): обойти записи newest→oldest и
    /// принять решение по ключу:
    /// - Tombstone → Tombstone
    /// - Валидная запись (TTL ок) → Present
    /// - Протухшая запись → продолжаем искать более старую версию на этой же странице
    /// - Ничего не нашли/всё протухло → Continue
    #[inline]
    fn page_decide_exists_for_key(&self, page: &[u8], key: &[u8], now: u32) -> DecideExists {
        let mut decision = DecideExists::Continue;

        kv_for_each_record(page, |k, _v, expires_at_sec, vflags| {
            if !matches!(decision, DecideExists::Continue) {
                return;
            }
            if k != key {
                return;
            }
            if (vflags & 0x1) == 1 {
                decision = DecideExists::Tombstone;
                return;
            }
            if expires_at_sec != 0 && now >= expires_at_sec {
                record_ttl_skipped();
                return;
            }
            decision = DecideExists::Present;
        });

        decision
    }
}

// ---------- локальные типы ----------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecideExists {
    Tombstone,
    Present,
    Continue,
}

// ---------- локальные хелперы ----------

#[inline]
fn data_end_for_header(hdr: &crate::page::kv::KvHeaderV3, ps: usize) -> usize {
    if hdr.table_slots == 0 {
        ps.saturating_sub(TRAILER_LEN)
    } else {
        ps.saturating_sub(TRAILER_LEN + (hdr.table_slots as usize) * KV_SLOT_SIZE)
    }
}