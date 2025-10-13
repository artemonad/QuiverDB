//! db/kv — одиночные операции put/get/del для Db.
//!
//! Что внутри:
//! - put: для малых значений — одна KV‑страница; для больших — OVERFLOW3 цепочка + KV.
//! - del: пишет tombstone.
//! - get: tail-wins, tombstone приоритетен, read-side TTL; разворачивает OVERFLOW placeholder.
//!
//! Fast paths (read):
//! - In‑memory keydir (если построен при RO): O(1) путь (pid/NO_PAGE).
//! - Bloom positive → лёгкий префетч головы (pager.prefetch_page(head)).

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};

use crate::metrics::{
    record_ttl_skipped,
    record_bloom_negative,
    record_bloom_positive,
    record_bloom_skipped_stale,
};
use crate::page::{
    kv_init_v3, kv_header_read_v3, kv_header_write_v3,
    ovf_header_read_v3, ovf_init_v3, ovf_header_write_v3,
    PAGE_MAGIC, PAGE_TYPE_KV_RH3,
    KV_HDR_MIN, TRAILER_LEN, OVF_HDR_MIN,
};
use crate::page::kv::kv_find_record_by_key;
use crate::page::ovf::chain as page_ovf_chain;
use crate::dir::NO_PAGE;
use crate::bloom::BloomSidecar;
use crate::util::{now_secs, decode_ovf_placeholder_v3};

use super::core::Db;

// ----------------- публичные методы -----------------

impl Db {
    /// Записать ключ/значение.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.readonly {
            return Err(anyhow!("Db is read-only"));
        }
        if key.len() > u16::MAX as usize {
            return Err(anyhow!("key too long (> u16::MAX)"));
        }

        let bucket = self.dir.bucket_of_key(key, self.pager.meta.hash_kind);
        let old_head = self.dir.head(bucket)?;
        let ps = self.pager.meta.page_size as usize;

        // Малое значение — одна KV‑страница, HEADS_UPDATE в одном батче
        if self.inline_fits_one_record(ps, key, value) {
            let new_pid = self.pager.allocate_one_page()?;
            let mut page = vec![0u8; ps];
            kv_init_v3(&mut page, new_pid, 0)?;
            self.write_single_record_kv_page(&mut page, key, value)?;
            {
                let mut h = kv_header_read_v3(&page)?;
                h.next_page_id = old_head;
                kv_header_write_v3(&mut page, &h)?;
            }

            let mut for_commit: Vec<(u64, &mut [u8])> = vec![(new_pid, page.as_mut_slice())];
            let updates: Vec<(u32, u64)> = vec![(bucket, new_pid)];
            self.pager
                .commit_pages_batch_with_heads(&mut for_commit, &updates)?;
            self.dir.set_head(bucket, new_pid)?;
            return Ok(());
        }

        // Большое значение: OVERFLOW3 + KV placeholder
        let (ovf_head, mut ovf_pages) = self.build_overflow_chain_pages(value)?;
        let new_kv_pid = self.pager.allocate_one_page()?;
        let mut kv_page = vec![0u8; ps];
        kv_init_v3(&mut kv_page, new_kv_pid, 0)?;
        let placeholder = make_ovf_placeholder_v3(value.len() as u64, ovf_head);
        self.write_single_record_kv_page(&mut kv_page, key, &placeholder)?;
        {
            let mut h = kv_header_read_v3(&mut kv_page)?;
            h.next_page_id = old_head;
            kv_header_write_v3(&mut kv_page, &h)?;
        }

        let mut for_commit: Vec<(u64, &mut [u8])> =
            Vec::with_capacity(ovf_pages.len() + 1);
        for (pid, buf) in ovf_pages.iter_mut() {
            for_commit.push((*pid, buf.as_mut_slice()));
        }
        for_commit.push((new_kv_pid, kv_page.as_mut_slice()));

        let updates: Vec<(u32, u64)> = vec![(bucket, new_kv_pid)];
        self.pager
            .commit_pages_batch_with_heads(&mut for_commit, &updates)?;
        self.dir.set_head(bucket, new_kv_pid)?;
        Ok(())
    }

    /// Удалить ключ — пишет tombstone.
    pub fn del(&mut self, key: &[u8]) -> Result<bool> {
        if self.readonly {
            return Err(anyhow!("Db is read-only"));
        }
        if key.len() > u16::MAX as usize {
            return Err(anyhow!("key too long (> u16::MAX)"));
        }
        let bucket = self.dir.bucket_of_key(key, self.pager.meta.hash_kind);
        let old_head = self.dir.head(bucket)?;
        let existed = old_head != NO_PAGE;

        let ps = self.pager.meta.page_size as usize;
        let new_pid = self.pager.allocate_one_page()?;
        let mut page = vec![0u8; ps];
        kv_init_v3(&mut page, new_pid, 0)?;
        self.write_single_record_kv_page_with_flags(&mut page, key, &[], 0, 1)?;
        {
            let mut h = kv_header_read_v3(&mut page)?;
            h.next_page_id = old_head;
            kv_header_write_v3(&mut page, &h)?;
        }

        let mut for_commit: Vec<(u64, &mut [u8])> = vec![(new_pid, page.as_mut_slice())];
        let updates: Vec<(u32, u64)> = vec![(bucket, new_pid)];
        self.pager
            .commit_pages_batch_with_heads(&mut for_commit, &updates)?;
        self.dir.set_head(bucket, new_pid)?;
        Ok(existed)
    }

    /// Получить значение по ключу.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let bucket = self.dir.bucket_of_key(key, self.pager.meta.hash_kind);
        let ps = self.pager.meta.page_size as usize;
        let now = now_secs();

        // Единый переиспользуемый буфер страницы
        let mut page_buf = vec![0u8; ps];

        // Fast path: in‑memory keydir
        if let Some(pid) = self.mem_keydir_get(bucket, key) {
            // Tombstone — немедленно None
            if pid == NO_PAGE {
                return Ok(None);
            }
            if let Some(v) = self.get_from_single_page_or_fallback_with_buf(key, pid, now, &mut page_buf)? {
                return Ok(Some(v));
            }
            // Если вернулся None — fallback выполнит обычный путь ниже
        }

        // Bloom fast‑path (кэшированный sidecar)
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
                    return Ok(None);
                }
            } else {
                record_bloom_skipped_stale();
            }
        } else {
            record_bloom_skipped_stale();
        }

        // Обычный обход цепочки
        let pid = self.dir.head(bucket)?;
        if pid == NO_PAGE {
            return Ok(None);
        }
        self.get_from_chain_starting_at_with_buf(key, pid, now, &mut page_buf)
    }
}

// ----------------- приватные помощники без логирования -----------------

impl Db {
    // Подходит ли запись целиком (одна) в KV‑страницу (без слотов)?
    fn inline_fits_one_record(&self, ps: usize, key: &[u8], value: &[u8]) -> bool {
        if let Some(thr) = self.pager.ovf_threshold_bytes {
            if value.len() > thr {
                return false;
            }
        }
        let overhead = KV_HDR_MIN + TRAILER_LEN + 2 + 4 + 4 + 1;
        overhead + key.len() + value.len() <= ps
    }

    pub(crate) fn write_single_record_kv_page(&self, page: &mut [u8], key: &[u8], value: &[u8]) -> Result<()> {
        self.write_single_record_kv_page_with_flags(page, key, value, 0, 0)
    }

    pub(crate) fn write_single_record_kv_page_with_flags(
        &self,
        page: &mut [u8],
        key: &[u8],
        value: &[u8],
        expires_at_sec: u32,
        vflags: u8,
    ) -> Result<()> {
        if page.len() < KV_HDR_MIN + TRAILER_LEN {
            return Err(anyhow!("page too small for KV record"));
        }
        let off = KV_HDR_MIN;
        let need = off + 2 + 4 + 4 + 1 + key.len() + value.len();
        if need > page.len() - TRAILER_LEN {
            return Err(anyhow!("record does not fit in page"));
        }
        LittleEndian::write_u16(&mut page[off..off + 2], key.len() as u16);
        LittleEndian::write_u32(&mut page[off + 2..off + 6], value.len() as u32);
        LittleEndian::write_u32(&mut page[off + 6..off + 10], expires_at_sec);
        page[off + 10] = vflags;
        let base = off + 11;
        page[base..base + key.len()].copy_from_slice(key);
        page[base + key.len()..base + key.len() + value.len()].copy_from_slice(value);

        let mut h = kv_header_read_v3(page)?;
        h.data_start = (base + key.len() + value.len()) as u32;
        kv_header_write_v3(page, &h)?;
        Ok(())
    }

    fn build_overflow_chain_pages(&mut self, value: &[u8]) -> Result<(u64, Vec<(u64, Vec<u8>)>)> {
        let ps = self.pager.meta.page_size as usize;
        let header_min = OVF_HDR_MIN;
        let cap = ps - header_min - TRAILER_LEN;
        if cap == 0 {
            return Err(anyhow!("overflow page capacity is zero"));
        }

        let chunks: Vec<&[u8]> = value.chunks(cap).collect();
        let n = chunks.len();
        let start_pid = self.pager.allocate_pages(n as u64)?;

        let codec_default = self.pager.meta.codec_default; // 0=none, 1=zstd
        let mut out: Vec<(u64, Vec<u8>)> = Vec::with_capacity(n);

        for i in 0..n {
            let pid = start_pid + i as u64;
            let next = if i + 1 < n { start_pid + i as u64 + 1 } else { NO_PAGE };

            let (codec_id, payload) = if codec_default == 1 {
                match compress_zstd(chunks[i]) {
                    Ok(comp) if comp.len() <= cap => (1u16, comp),
                    _ => (0u16, chunks[i].to_vec()),
                }
            } else {
                (0u16, chunks[i].to_vec())
            };

            let mut page = vec![0u8; ps];
            ovf_init_v3(&mut page, pid, codec_id)?;
            {
                let mut h = ovf_header_read_v3(&page)?;
                h.chunk_len = payload.len() as u32;
                h.next_page_id = next;
                ovf_header_write_v3(&mut page, &h)?;
            }
            let base = header_min;
            page[base..base + payload.len()].copy_from_slice(&payload);
            out.push((pid, page));
        }

        Ok((start_pid, out))
    }

    fn read_overflow_chain(&self, head: u64, expected_len: usize) -> Result<Vec<u8>> {
        page_ovf_chain::read_overflow_chain(&self.pager, head, expected_len)
    }

    /// Проверка по одной странице pid с возможным fallback от next_pid.
    /// Возвращает:
    /// - Some(true/false value) — удалось принять решение сразу (включая fallback)
    /// - None — если требуется продолжать обычный путь (редкий случай невалидного кадра/тип страницы)
    fn get_from_single_page_or_fallback_with_buf(
        &self,
        key: &[u8],
        pid: u64,
        now: u32,
        page_buf: &mut [u8],
    ) -> Result<Option<Vec<u8>>> {
        if pid == NO_PAGE {
            return Ok(None);
        }

        let ps = self.pager.meta.page_size as usize;
        debug_assert_eq!(page_buf.len(), ps);

        self.pager.read_page(pid, page_buf)?;
        if &page_buf[0..4] != PAGE_MAGIC {
            return self.get_from_chain_starting_at_with_buf(key, pid, now, page_buf);
        }
        let ptype = LittleEndian::read_u16(&page_buf[6..8]);
        if ptype != PAGE_TYPE_KV_RH3 {
            return self.get_from_chain_starting_at_with_buf(key, pid, now, page_buf);
        }

        let h = kv_header_read_v3(page_buf)?;

        // На странице попробуем точечный поиск по ключу
        if let Some((_k, v, expires_at_sec, vflags)) = kv_find_record_by_key(page_buf, key) {
            // Tombstone — немедленно None
            if (vflags & 0x1) == 1 {
                return Ok(None);
            }
            // TTL
            if expires_at_sec != 0 && now >= expires_at_sec {
                record_ttl_skipped();
                // Ищем глубже по цепочке
                return self.get_from_chain_starting_at_with_buf(key, h.next_page_id, now, page_buf);
            }
            // Валидная запись
            if let Some((total_len, head_pid)) = decode_ovf_placeholder_v3(v) {
                let val = self.read_overflow_chain(head_pid, total_len as usize)?;
                return Ok(Some(val));
            } else {
                return Ok(Some(v.to_vec()));
            }
        }

        // Запись для ключа не найдена на этой странице — перейдём на обычный путь от next_page_id
        self.get_from_chain_starting_at_with_buf(key, h.next_page_id, now, page_buf)
    }

    /// Линейный обход цепочки от произвольного pid с TTL/tombstone семантикой.
    fn get_from_chain_starting_at_with_buf(
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

            let h = kv_header_read_v3(page_buf)?;

            if let Some((_k, v, expires_at_sec, vflags)) = kv_find_record_by_key(page_buf, key) {
                // Tombstone — отсутствует
                if (vflags & 0x1) == 1 {
                    return Ok(None);
                }
                // TTL
                if expires_at_sec != 0 && now >= expires_at_sec {
                    record_ttl_skipped();
                } else {
                    // Нашли валидную запись
                    if let Some((total_len, head_pid)) = decode_ovf_placeholder_v3(v) {
                        let val = self.read_overflow_chain(head_pid, total_len as usize)?;
                        return Ok(Some(val));
                    } else {
                        return Ok(Some(v.to_vec()));
                    }
                }
            }

            pid = h.next_page_id;
        }
        Ok(None)
    }
}

// ----------------- TLV placeholder (OVF_CHAIN) -----------------

fn make_ovf_placeholder_v3(total_len: u64, head_pid: u64) -> Vec<u8> {
    let mut out = vec![0u8; 1 + 1 + 16];
    out[0] = 0x01;
    out[1] = 16;
    LittleEndian::write_u64(&mut out[2..10], total_len);
    LittleEndian::write_u64(&mut out[10..18], head_pid);
    out
}

fn compress_zstd(bytes: &[u8]) -> Result<Vec<u8>> {
    Ok(zstd::bulk::compress(bytes, 0)?)
}