//! db/scan — сканы ключей: scan_all / scan_prefix / потоковый scan_stream.
//!
//! Обновлено (packed-aware):
//! - Chain-based скан использует обход всех записей на странице, а не только одну.
//! - Внутри страницы слоты обходятся в обратном порядке (последним добавленным — приоритет),
//!   чтобы при наличии нескольких версий одного ключа на одной странице “новая” версия побеждала.
//! - Быстрый путь через keydir использует packed-aware поиск kv_find_record_by_key.
//!
//! NEW (perf):
//! - Один буфер страницы на весь вызов (и для keydir‑fast‑path, и для chain‑scan), без переаллоков.
//!
//! Семантика неизменна:
//! - tail-wins: идём от head к хвосту, "побеждает" первый валидный (не tombstone, не истёкший TTL).
//! - Tombstone имеет приоритет.
//! - Read-side TTL: expires_at_sec=0 — бессрочно; если now >= expires_at_sec — запись пропускается.

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use std::collections::{HashMap, HashSet};

use crate::dir::NO_PAGE;
use crate::metrics::record_ttl_skipped;
use crate::page::{
    kv_header_read_v3, PAGE_MAGIC, PAGE_TYPE_KV_RH3, KV_HDR_MIN,
};
// NEW: packed-aware точечный поиск (подмодуль kv)
use crate::page::kv::kv_find_record_by_key;
// Для безопасной ручной итерации по слотам
use crate::page::common::{TRAILER_LEN, KV_SLOT_SIZE, KV_EMPTY_OFF};
// Общий ридер OVERFLOW-цепочек
use crate::page::ovf::chain as page_ovf_chain;
// Централизованные утилиты
use crate::util::{now_secs, decode_ovf_placeholder_v3};

use super::core::Db;

impl Db {
    /// Собрать все пары (ключ, значение) по всем бакетам.
    /// Если ключ встречается несколько раз в цепочке, "побеждает" первый валидный от head.
    pub fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if self.has_mem_keydir() {
            self.scan_materialized_via_keydir(None)
        } else {
            self.scan_materialized_via_chains(None)
        }
    }

    /// Собрать пары (ключ, значение) только для ключей с заданным префиксом.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if self.has_mem_keydir() {
            self.scan_materialized_via_keydir(Some(prefix))
        } else {
            self.scan_materialized_via_chains(Some(prefix))
        }
    }

    /// Потоковый скан: вызывает cb для каждой финальной пары.
    /// Если prefix=None — полный скан; иначе — только ключи с заданным префиксом.
    pub fn scan_stream<F>(&self, prefix: Option<&[u8]>, cb: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]),
    {
        if self.has_mem_keydir() {
            self.scan_stream_via_keydir(prefix, cb)
        } else {
            self.scan_stream_via_chains(prefix, cb)
        }
    }

    // -------------------- keydir fast-path (с единым буфером) --------------------

    fn scan_stream_via_keydir<F>(&self, prefix: Option<&[u8]>, mut cb: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let now = now_secs();
        let ps = self.pager.meta.page_size as usize;
        let mut page_buf = vec![0u8; ps];

        match prefix {
            None => {
                self.mem_keydir_for_each(|_b, k, pid| {
                    if pid == NO_PAGE {
                        return;
                    }
                    if let Ok(Some(v)) = self.value_from_pid_or_fallback_with_buf(k, pid, now, &mut page_buf) {
                        cb(k, &v);
                    }
                });
            }
            Some(pref) => {
                self.mem_keydir_for_each_prefix(pref, |_b, k, pid| {
                    if pid == NO_PAGE {
                        return;
                    }
                    if let Ok(Some(v)) = self.value_from_pid_or_fallback_with_buf(k, pid, now, &mut page_buf) {
                        cb(k, &v);
                    }
                });
            }
        }
        Ok(())
    }

    fn scan_materialized_via_keydir(
        &self,
        prefix: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        self.scan_stream_via_keydir(prefix, |k, v| out.push((k.to_vec(), v.to_vec())))?;
        Ok(out)
    }

    // -------------------- chain-based (packed-aware, единый буфер) --------------------

    fn scan_stream_via_chains<F>(&self, prefix: Option<&[u8]>, mut cb: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]),
    {
        let ps = self.pager.meta.page_size as usize;
        let now = now_secs();

        // финальное состояние для ключей:
        enum State {
            Selected, // уже отдан пользователю
            Deleted,  // tombstone от head
        }

        let mut state: HashMap<Vec<u8>, State> = HashMap::new();
        let mut page = vec![0u8; ps];

        for b in 0..self.dir.bucket_count {
            let mut pid = self.dir.head(b)?;
            while pid != NO_PAGE {
                self.pager.read_page(pid, &mut page)?;
                if &page[0..4] != PAGE_MAGIC {
                    break;
                }
                let ptype = LittleEndian::read_u16(&page[6..8]);
                if ptype != PAGE_TYPE_KV_RH3 {
                    break;
                }

                let h = kv_header_read_v3(&page)?;
                // Верхняя граница data-area
                let data_end = data_end_for_page(&h, ps).unwrap_or(0);

                // Обход всех записей страницы в порядке "новые → старые"
                for_each_records_newest_first(&page, data_end, |k, v, expires_at_sec, vflags| {
                    // Если по ключу уже принято решение — пропускаем
                    if state.contains_key(k) {
                        return;
                    }
                    // Префиксный фильтр
                    if let Some(pref) = prefix {
                        if !k.starts_with(pref) {
                            return;
                        }
                    }

                    let is_tomb = (vflags & 0x1) == 1;
                    let ttl_ok = expires_at_sec == 0 || now < expires_at_sec;

                    if is_tomb {
                        // Tombstone имеет приоритет — фиксируем удаление
                        state.insert(k.to_vec(), State::Deleted);
                    } else if ttl_ok {
                        // Валидная запись → раскрываем placeholder при необходимости
                        if let Ok(value_bytes) = self.expand_value_if_needed(v) {
                            cb(k, &value_bytes);
                            state.insert(k.to_vec(), State::Selected);
                        }
                    } else {
                        // Протухшая запись — считаем метрику и ищем глубже
                        record_ttl_skipped();
                    }
                });

                pid = h.next_page_id;
            }
        }

        Ok(())
    }

    fn scan_materialized_via_chains(
        &self,
        prefix: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        self.scan_stream_via_chains(prefix, |k, v| out.push((k.to_vec(), v.to_vec())))?;
        Ok(out)
    }

    // -------------------- общие хелперы (packed-aware + единый буфер) --------------------

    /// Внутренний помощник: вернуть значение ключа, начиная с pid (одна страница),
    /// либо продолжить обход, если на странице tombstone/протух/другой ключ.
    #[inline]
    fn value_from_pid_or_fallback_with_buf(
        &self,
        key: &[u8],
        pid: u64,
        now: u32,
        page_buf: &mut [u8],
    ) -> Result<Option<Vec<u8>>> {
        let mut cur = pid;
        loop {
            match self.read_one_kv_page_decide_with_buf(key, cur, now, page_buf)? {
                DecideResult::Value(v, _next) => return Ok(Some(v)),
                DecideResult::Tombstone(_next) => return Ok(None),
                DecideResult::Continue(next) => {
                    if next == NO_PAGE {
                        return Ok(None);
                    }
                    cur = next;
                }
            }
        }
    }

    /// Решение по одной KV‑странице для конкретного ключа (packed-aware, использует kv_find_record_by_key).
    #[inline]
    fn read_one_kv_page_decide_with_buf(
        &self,
        key: &[u8],
        pid: u64,
        now: u32,
        page_buf: &mut [u8],
    ) -> Result<DecideResult> {
        if pid == NO_PAGE {
            return Ok(DecideResult::Continue(NO_PAGE));
        }
        let ps = self.pager.meta.page_size as usize;
        debug_assert_eq!(ps, page_buf.len());

        self.pager.read_page(pid, page_buf)?;
        if &page_buf[0..4] != PAGE_MAGIC {
            return Ok(DecideResult::Continue(NO_PAGE));
        }
        let ptype = LittleEndian::read_u16(&page_buf[6..8]);
        if ptype != PAGE_TYPE_KV_RH3 {
            return Ok(DecideResult::Continue(NO_PAGE));
        }

        let h = kv_header_read_v3(page_buf)?;
        let next = h.next_page_id;

        // packed-aware: точечный поиск записи по ключу на странице
        if let Some((_k, v, expires_at_sec, vflags)) = kv_find_record_by_key(page_buf, key) {
            if (vflags & 0x1) == 1 {
                return Ok(DecideResult::Tombstone(next));
            }
            if expires_at_sec != 0 && now >= expires_at_sec {
                record_ttl_skipped();
                return Ok(DecideResult::Continue(next));
            }
            let val = self.expand_value_if_needed(v)?;
            return Ok(DecideResult::Value(val, next));
        }

        Ok(DecideResult::Continue(next))
    }

    /// Раскрыть значение (OVERFLOW placeholder → байты).
    #[inline]
    fn expand_value_if_needed(&self, v: &[u8]) -> Result<Vec<u8>> {
        if let Some((total_len, head_pid)) = decode_ovf_placeholder_v3(v) {
            page_ovf_chain::read_overflow_chain(&self.pager, head_pid, total_len as usize)
        } else {
            Ok(v.to_vec())
        }
    }
}

// -------------------- локальные помощники --------------------

#[derive(Debug)]
enum DecideResult {
    Value(Vec<u8>, u64), // value, next_pid
    Tombstone(u64),      // next_pid
    Continue(u64),       // next_pid
}

// -------------------- безопасные хелперы для packed-страниц --------------------

/// Верхняя граница data‑area (до slot‑таблицы). None при переполнении вычислений.
#[inline]
fn data_end_for_page(hdr: &crate::page::kv::KvHeaderV3, ps: usize) -> Option<usize> {
    if hdr.table_slots == 0 {
        ps.checked_sub(TRAILER_LEN)
    } else {
        ps.checked_sub(TRAILER_LEN + (hdr.table_slots as usize) * KV_SLOT_SIZE)
    }
}

/// Прочитать запись по произвольному смещению off с учётом верхней границы data_end.
/// Возвращает None, если запись целиком не помещается в data‑area.
#[inline]
fn read_record_at_checked<'a>(
    page: &'a [u8],
    off: usize,
    data_end: usize,
) -> Option<(&'a [u8], &'a [u8], u32, u8)> {
    // klen u16 + vlen u32 + expires u32 + vflags у8
    if off.checked_add(11)? > data_end {
        return None;
    }
    let klen = LittleEndian::read_u16(&page[off..off + 2]) as usize;
    let vlen = LittleEndian::read_u32(&page[off + 2..off + 6]) as usize;
    let expires_at_sec = LittleEndian::read_u32(&page[off + 6..off + 10]);
    let vflags = page[off + 10];

    let base = off + 11;
    let end = base.checked_add(klen)?.checked_add(vlen)?;
    if end > data_end {
        return None;
    }
    let key = &page[base..base + klen];
    let val = &page[base + klen..base + klen + vlen];
    Some((key, val, expires_at_sec, vflags))
}

/// Обойти все записи на странице в порядке "новые → старые" (reverse слоты).
fn for_each_records_newest_first<'a, F>(
    page: &'a [u8],
    data_end: usize,
    mut f: F,
) where
    F: FnMut(&'a [u8], &'a [u8], u32, u8),
{
    // Валидация базовой геометрии
    if page.len() < KV_HDR_MIN + TRAILER_LEN {
        return;
    }
    if &page[0..4] != PAGE_MAGIC {
        return;
    }
    let ptype = LittleEndian::read_u16(&page[6..8]);
    if ptype != PAGE_TYPE_KV_RH3 {
        return;
    }

    // Прочитаем заголовок
    let hdr = match kv_header_read_v3(page) {
        Ok(h) => h,
        Err(_) => return,
    };

    if hdr.table_slots == 0 {
        // Одиночная запись — проверим, что помещается в data_end.
        if let Some((k, v, e, fl)) = read_record_at_checked(page, KV_HDR_MIN, data_end) {
            f(k, v, e, fl);
        }
        return;
    }

    // Обойдем слоты с конца к началу, чтобы приоритет был у новых записей
    let ps = page.len();
    let table_slots = hdr.table_slots as usize;
    let table_start = match ps.checked_sub(TRAILER_LEN + table_slots * KV_SLOT_SIZE) {
        Some(v) => v,
        None => return,
    };

    let mut seen_off: HashSet<u32> = HashSet::new();

    for i in (0..table_slots).rev() {
        let slot_off = table_start + i * KV_SLOT_SIZE;
        if slot_off + KV_SLOT_SIZE > ps.saturating_sub(TRAILER_LEN) {
            break;
        }
        let off = LittleEndian::read_u32(&page[slot_off..slot_off + 4]);
        if off == KV_EMPTY_OFF || !seen_off.insert(off) {
            continue;
        }
        let off_usize = off as usize;
        if let Some((k, v, e, fl)) = read_record_at_checked(page, off_usize, data_end) {
            f(k, v, e, fl);
        }
    }
}