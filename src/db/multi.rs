//! db/multi — векторные операции get_many/exists_many.
//!
//! Идея:
//! - Сгруппировать запросы по page_id и читать каждую страницу ровно один раз.
//! - Для ключей с in‑memory keydir (pid, off) — прочитать запись по точному смещению (safe).
//! - Для остальных — packed‑поиск на странице (decide_value_on_page) и, при необходимости, fallback
//!   по цепочке head→tail (LSN/TTL/tombstone семантика как в обычном get/exists).
//!
//! Семантика идентична одиночным get/exists:
//! - Tombstone имеет приоритет.
//! - TTL read‑side: expires_at_sec == 0 (бессрочно) или now < expires_at_sec.
//! - Внутристраничный поиск — packed‑aware newest→oldest.
//! - OVERFLOW placeholder: get_many разворачивает цепочку (с value cache); exists_many — считает “present” без разворота.

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use std::collections::BTreeMap;

use crate::db::read_page::{decide_value_on_page, DecideOnPage};
use crate::dir::NO_PAGE;
use crate::page::kv::kv_read_record_at_checked;
use crate::page::ovf::chain as page_ovf_chain;
use crate::page::{kv_header_read_v3, PAGE_MAGIC, PAGE_TYPE_KV_RH3, TRAILER_LEN};
use crate::util::{decode_ovf_placeholder_v3, now_secs};
// NEW: value cache для OVERFLOW
use crate::pager::value_cache::{value_cache_get, value_cache_put};

use super::core::Db;

impl Db {
    /// Векторный get: семантика как у одиночного get().
    pub fn get_many<'a>(&self, keys: &[&'a [u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        let now = now_secs();
        let ps = self.pager.meta.page_size as usize;

        // Группировка по page_id.
        // Для ключей с tombstone в keydir (pid=NO_PAGE) — сразу None.
        #[derive(Clone)]
        struct Req<'a> {
            idx: usize,
            key: &'a [u8],
            _pid: u64,
            off: u32,
            _bucket: u32,
        }

        let mut by_pid: BTreeMap<u64, Vec<Req>> = BTreeMap::new();
        let mut out: Vec<Option<Vec<u8>>> = vec![None; keys.len()];

        for (i, key) in keys.iter().enumerate() {
            let bucket = self.dir.bucket_of_key(key, self.pager.meta.hash_kind);

            if let Some(loc) = self.mem_keydir_get_loc(bucket, key) {
                // tombstone в keydir → None немедленно
                if loc.pid == NO_PAGE {
                    out[i] = None;
                    continue;
                }
                by_pid.entry(loc.pid).or_default().push(Req {
                    idx: i,
                    key,
                    _pid: loc.pid,
                    off: loc.off,
                    _bucket: bucket,
                });
            } else {
                // нет keydir или нет записи — fallback на head
                let pid = self.dir.head(bucket)?;
                if pid == NO_PAGE {
                    out[i] = None;
                    continue;
                }
                by_pid.entry(pid).or_default().push(Req {
                    idx: i,
                    key,
                    _pid: pid,
                    off: 0,
                    _bucket: bucket,
                });
            }
        }

        let mut page_buf = vec![0u8; ps];

        for (pid, reqs) in by_pid.into_iter() {
            // Считываем страницу один раз
            self.pager.read_page(pid, &mut page_buf)?;

            if &page_buf[0..4] != PAGE_MAGIC {
                // страницa невалидна — безопасный fallback по цепочке (для каждого запроса)
                for req in reqs {
                    out[req.idx] = self.scan_chain_get(req.key, pid, now, &mut page_buf)?;
                }
                continue;
            }
            let ptype = LittleEndian::read_u16(&page_buf[6..8]);
            if ptype != PAGE_TYPE_KV_RH3 {
                // не KV — fallback
                for req in reqs {
                    out[req.idx] = self.scan_chain_get(req.key, pid, now, &mut page_buf)?;
                }
                continue;
            }

            let hdr = kv_header_read_v3(&page_buf)?;
            let next_pid = hdr.next_page_id;
            let data_end = data_end_for_page(&hdr, ps).unwrap_or(ps.saturating_sub(TRAILER_LEN));

            for req in reqs {
                let idx = req.idx;
                // 1) Быстрый путь: по точному off (если задан)
                if req.off != 0 {
                    if let Some((k, v, exp, fl)) =
                        kv_read_record_at_checked(&page_buf, req.off as usize, data_end)
                    {
                        if k == req.key {
                            // tombstone — None
                            if (fl & 0x1) == 1 {
                                out[idx] = None;
                                continue;
                            }
                            // TTL
                            if exp != 0 && now >= exp {
                                // протухло → fallback с хвоста страницы
                                out[idx] =
                                    self.scan_chain_get(req.key, next_pid, now, &mut page_buf)?;
                                continue;
                            }
                            // Валидно: inline или OVERFLOW placeholder (+ value cache)
                            let val = self.expand_value_cached_for_multi(v)?;
                            out[idx] = Some(val);
                            continue;
                        }
                        // рассинхрон по ключу — пойдём общим путём по странице
                    }
                    // Если off не сработал — общий packed‑поиск на текущей странице
                }

                // 2) Общее решение по странице
                match decide_value_on_page(&page_buf, req.key, now) {
                    DecideOnPage::Tombstone => {
                        out[idx] = None;
                    }
                    DecideOnPage::Valid(v) => {
                        out[idx] = Some(v);
                    }
                    DecideOnPage::NeedOverflow {
                        total_len,
                        head_pid,
                    } => {
                        // value cache
                        if let Some(cached) = value_cache_get(self.pager.db_id, head_pid, total_len)
                        {
                            out[idx] = Some(cached);
                        } else {
                            let val = page_ovf_chain::read_overflow_chain(
                                &self.pager,
                                head_pid,
                                total_len,
                            )?;
                            value_cache_put(self.pager.db_id, head_pid, total_len, &val);
                            out[idx] = Some(val);
                        }
                    }
                    DecideOnPage::Continue => {
                        // 3) Fallback: продолжаем цепочку
                        out[idx] = self.scan_chain_get(req.key, next_pid, now, &mut page_buf)?;
                    }
                }
            }
        }

        Ok(out)
    }

    /// Векторный exists: семантика как у одиночного exists().
    pub fn exists_many<'a>(&self, keys: &[&'a [u8]]) -> Result<Vec<bool>> {
        let now = now_secs();
        let ps = self.pager.meta.page_size as usize;

        #[derive(Clone)]
        struct Req<'a> {
            idx: usize,
            key: &'a [u8],
            _pid: u64,
            off: u32,
            _bucket: u32,
        }

        let mut by_pid: BTreeMap<u64, Vec<Req>> = BTreeMap::new();
        let mut out: Vec<bool> = vec![false; keys.len()];

        for (i, key) in keys.iter().enumerate() {
            let bucket = self.dir.bucket_of_key(key, self.pager.meta.hash_kind);

            if let Some(loc) = self.mem_keydir_get_loc(bucket, key) {
                if loc.pid == NO_PAGE {
                    out[i] = false;
                    continue;
                }
                by_pid.entry(loc.pid).or_default().push(Req {
                    idx: i,
                    key,
                    _pid: loc.pid,
                    off: loc.off,
                    _bucket: bucket,
                });
            } else {
                let pid = self.dir.head(bucket)?;
                if pid == NO_PAGE {
                    out[i] = false;
                    continue;
                }
                by_pid.entry(pid).or_default().push(Req {
                    idx: i,
                    key,
                    _pid: pid,
                    off: 0,
                    _bucket: bucket,
                });
            }
        }

        let mut page_buf = vec![0u8; ps];

        for (pid, reqs) in by_pid.into_iter() {
            self.pager.read_page(pid, &mut page_buf)?;

            if &page_buf[0..4] != PAGE_MAGIC {
                for req in reqs {
                    out[req.idx] = self.scan_chain_exists(req.key, pid, now, &mut page_buf)?;
                }
                continue;
            }
            let ptype = LittleEndian::read_u16(&page_buf[6..8]);
            if ptype != PAGE_TYPE_KV_RH3 {
                for req in reqs {
                    out[req.idx] = self.scan_chain_exists(req.key, pid, now, &mut page_buf)?;
                }
                continue;
            }

            let hdr = kv_header_read_v3(&page_buf)?;
            let next_pid = hdr.next_page_id;
            let data_end = data_end_for_page(&hdr, ps).unwrap_or(ps.saturating_sub(TRAILER_LEN));

            for req in reqs {
                let idx = req.idx;

                if req.off != 0 {
                    if let Some((k, _v, exp, fl)) =
                        kv_read_record_at_checked(&page_buf, req.off as usize, data_end)
                    {
                        if k == req.key {
                            if (fl & 0x1) == 1 {
                                out[idx] = false;
                                continue;
                            }
                            if exp != 0 && now >= exp {
                                out[idx] =
                                    self.scan_chain_exists(req.key, next_pid, now, &mut page_buf)?;
                                continue;
                            }
                            // Валидная запись на off — present
                            out[idx] = true;
                            continue;
                        }
                    }
                }

                match crate::db::read_page::decide_exists_on_page(&page_buf, req.key, now) {
                    crate::db::read_page::DecideExists::Tombstone => out[idx] = false,
                    crate::db::read_page::DecideExists::Present => out[idx] = true,
                    crate::db::read_page::DecideExists::Continue => {
                        out[idx] = self.scan_chain_exists(req.key, next_pid, now, &mut page_buf)?;
                    }
                }
            }
        }

        Ok(out)
    }

    // --------- внутренние fallback‑helpers ---------

    fn scan_chain_get(
        &self,
        key: &[u8],
        mut pid: u64,
        now: u32,
        page_buf: &mut [u8],
    ) -> Result<Option<Vec<u8>>> {
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

            let h = kv_header_read_v3(&page_buf)?;
            let next = h.next_page_id;

            match decide_value_on_page(page_buf, key, now) {
                DecideOnPage::Tombstone => return Ok(None),
                DecideOnPage::Valid(v) => return Ok(Some(v)),
                DecideOnPage::NeedOverflow {
                    total_len,
                    head_pid,
                } => {
                    // value cache
                    if let Some(cached) = value_cache_get(self.pager.db_id, head_pid, total_len) {
                        return Ok(Some(cached));
                    }
                    let val =
                        page_ovf_chain::read_overflow_chain(&self.pager, head_pid, total_len)?;
                    value_cache_put(self.pager.db_id, head_pid, total_len, &val);
                    return Ok(Some(val));
                }
                DecideOnPage::Continue => {
                    pid = next;
                }
            }
        }
        Ok(None)
    }

    fn scan_chain_exists(
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

            let h = kv_header_read_v3(&page_buf)?;
            let next = h.next_page_id;

            match crate::db::read_page::decide_exists_on_page(page_buf, key, now) {
                crate::db::read_page::DecideExists::Tombstone => return Ok(false),
                crate::db::read_page::DecideExists::Present => return Ok(true),
                crate::db::read_page::DecideExists::Continue => {
                    pid = next;
                }
            }
        }
        Ok(false)
    }

    #[inline]
    fn expand_value_cached_for_multi(&self, v: &[u8]) -> Result<Vec<u8>> {
        if let Some((total_len, head_pid)) = decode_ovf_placeholder_v3(v) {
            if let Some(cached) = value_cache_get(self.pager.db_id, head_pid, total_len as usize) {
                return Ok(cached);
            }
            let val =
                page_ovf_chain::read_overflow_chain(&self.pager, head_pid, total_len as usize)?;
            value_cache_put(self.pager.db_id, head_pid, total_len as usize, &val);
            Ok(val)
        } else {
            Ok(v.to_vec())
        }
    }
}

// -------------------- локальные хелперы --------------------

#[inline]
fn data_end_for_page(hdr: &crate::page::kv::KvHeaderV3, ps: usize) -> Option<usize> {
    if hdr.table_slots == 0 {
        ps.checked_sub(TRAILER_LEN)
    } else {
        ps.checked_sub(TRAILER_LEN + (hdr.table_slots as usize) * crate::page::common::KV_SLOT_SIZE)
    }
}
