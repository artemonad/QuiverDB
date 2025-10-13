//! pager/commit — коммит страниц (WAL v2 + запись в сегменты).
//!
//! - commit_page: BEGIN → IMAGE → COMMIT (один fsync WAL), запись страницы.
//! - commit_pages_batch: BEGIN(start) → N×IMAGE → COMMIT(last) (один fsync WAL), запись всех страниц,
//!   truncate WAL, meta.last_lsn=last (в памяти).
//! - commit_pages_batch_with_heads: BEGIN → IMAGE* → HEADS_UPDATE → COMMIT (один fsync WAL).
//!
//! Оптимизация записи данных батча:
//! - Сегменты открываются по одному разу на батч; страницы сегмента пишутся через BufWriter
//!   (крупный буфер), отсортированно по offset. Это существенно снижает количество системных вызовов.
//!   Если data_fsync=true — выполняется flush и sync_all() по завершении записи сегмента.
//!
//! NEW (2.1 prep): TDE AES‑GCM tag в трейлере при включённом pager.tde_enabled.
//! - После установки LSN трейлер заполняется либо CRC32C (по умолчанию), либо AEAD‑tag (AES‑256‑GCM).
//! - Ключ подгружается лениво из EnvKeyProvider (см. pager/core.rs).
//!
//! NEW (perf): больше НЕ переписываем <root>/meta на каждом коммите.
//! - self.meta.last_lsn обновляется только в памяти; flush meta выполняется при Drop(Db)
//!   (clean_shutdown=true) или вручную через CLI checkpoint.
//!
//! NEW (2.3): пороговые fsync'и WAL (P1_WAL_FLUSH_EVERY / P1_WAL_FLUSH_BYTES) не должны срабатывать
//! внутри батча. Для этого мы явно оборачиваем последовательность BEGIN..COMMIT в
//! wal.start_batch() / wal.end_batch() — см. writer.rs.

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::collections::BTreeMap;
use std::io::{Seek, SeekFrom, Write};

use crate::page::{
    kv_header_read_v3, kv_header_write_v3, ovf_header_read_v3, ovf_header_write_v3,
    page_update_checksum, page_update_trailer_aead_with,
    PAGE_MAGIC, PAGE_TYPE_KV_RH3, PAGE_TYPE_OVERFLOW3, OFF_TYPE, KV_OFF_LSN, OVF_OFF_LSN, TRAILER_LEN,
};
use crate::wal::Wal;

use super::core::Pager;

impl Pager {
    /// Коммит одиночной страницы: WAL (BEGIN/IMAGE/COMMIT) + запись страницы.
    pub fn commit_page(&mut self, page_id: u64, page: &mut [u8]) -> Result<()> {
        if page.len() != self.meta.page_size as usize {
            return Err(anyhow!(
                "buffer size {} != page_size {}",
                page.len(),
                self.meta.page_size
            ));
        }
        if &page[..4] != PAGE_MAGIC {
            return Err(anyhow!("commit_page: bad page magic"));
        }

        // [1] LSN и трейлер (CRC32C или AEAD)
        let lsn = self.meta.last_lsn.wrapping_add(1);
        set_v3_page_lsn_mut(page, lsn)?;
        self.update_page_trailer(page_id, page, lsn)?;

        // [2] WAL: BEGIN → IMAGE → COMMIT, один fsync WAL
        let mut wal = Wal::open_for_append(&self.root)?;
        // подавляем пороговые fsync-и внутри батча
        wal.start_batch();
        wal.append_begin(lsn)?;
        wal.append_page_image(lsn, page_id, page)?;
        wal.append_commit(lsn)?;
        wal.end_batch();
        wal.fsync()?; // fsync WAL

        // [3] Запись в сегмент
        self.write_page_raw(page_id, page)?;

        // [4] Ротация WAL
        wal.maybe_truncate()?;

        // [5] Обновить last_lsn (ТОЛЬКО в памяти; без write_meta_overwrite)
        self.meta.last_lsn = lsn;
        Ok(())
    }

    /// Групповой коммит страниц: один WAL‑батч и одна fsync WAL.
    /// Запись страниц сгруппирована по сегментам — одно открытие/один fsync на сегмент.
    pub fn commit_pages_batch(&mut self, pages: &mut [(u64, &mut [u8])]) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }

        // [1] Присвоить LSN и обновить трейлер каждой странице
        let start_lsn = self.meta.last_lsn.wrapping_add(1);
        let mut cur_lsn = start_lsn;
        for (pid, page_ref) in pages.iter_mut() {
            let page: &mut [u8] = &mut *page_ref;
            if page.len() != self.meta.page_size as usize {
                return Err(anyhow!(
                    "buffer size {} != page_size {}",
                    page.len(),
                    self.meta.page_size
                ));
            }
            if &page[..4] != PAGE_MAGIC {
                return Err(anyhow!("commit_pages_batch: bad page magic"));
            }
            set_v3_page_lsn_mut(page, cur_lsn)?;
            self.update_page_trailer(*pid, page, cur_lsn)?;
            cur_lsn = cur_lsn.wrapping_add(1);
        }
        let last_lsn = cur_lsn.wrapping_sub(1);

        // [2] WAL батч: BEGIN → IMAGE* → COMMIT, один fsync WAL
        let mut wal = Wal::open_for_append(&self.root)?;
        wal.start_batch();
        wal.append_begin(start_lsn)?;
        let mut lsn_it = start_lsn;
        for (pid, page) in pages.iter_mut() {
            wal.append_page_image(lsn_it, *pid, *page)?;
            lsn_it = lsn_it.wrapping_add(1);
        }
        wal.append_commit(last_lsn)?;
        wal.end_batch();
        wal.fsync()?; // fsync WAL

        // [3] Запись страниц в сегменты (через BufWriter)
        write_pages_grouped_by_segment(self, pages)?;

        // [4] Ротация WAL
        wal.maybe_truncate()?;

        // [5] Обновить last_lsn (в памяти; без write_meta_overwrite)
        self.meta.last_lsn = last_lsn;
        Ok(())
    }

    /// Групповой коммит страниц + атомарные обновления head’ов в одном WAL‑батче:
    /// BEGIN(start) → IMAGE* → HEADS_UPDATE(updates) → COMMIT(last), один fsync WAL.
    pub fn commit_pages_batch_with_heads(
        &mut self,
        pages: &mut [(u64, &mut [u8])],
        dir_updates: &[(u32, u64)],
    ) -> Result<()> {
        if pages.is_empty() && dir_updates.is_empty() {
            return Ok(());
        }

        // [1] Присвоить LSN и трейлеры страницам
        let start_lsn = self.meta.last_lsn.wrapping_add(1);
        let mut cur_lsn = start_lsn;
        for (pid, page_ref) in pages.iter_mut() {
            let page: &mut [u8] = &mut *page_ref;
            if page.len() != self.meta.page_size as usize {
                return Err(anyhow!(
                    "buffer size {} != page_size {}",
                    page.len(),
                    self.meta.page_size
                ));
            }
            if &page[..4] != PAGE_MAGIC {
                return Err(anyhow!("commit_pages_batch_with_heads: bad page magic"));
            }
            set_v3_page_lsn_mut(page, cur_lsn)?;
            self.update_page_trailer(*pid, page, cur_lsn)?;
            cur_lsn = cur_lsn.wrapping_add(1);
        }
        let last_lsn = cur_lsn.wrapping_sub(1);

        // [2] WAL: BEGIN → IMAGE* → HEADS_UPDATE → COMMIT (один fsync)
        let mut wal = Wal::open_for_append(&self.root)?;
        wal.start_batch();
        wal.append_begin(start_lsn)?;
        let mut lsn_it = start_lsn;
        for (pid, page) in pages.iter_mut() {
            wal.append_page_image(lsn_it, *pid, *page)?;
            lsn_it = lsn_it.wrapping_add(1);
        }
        wal.append_heads_update(last_lsn, dir_updates)?;
        wal.append_commit(last_lsn)?;
        wal.end_batch();
        wal.fsync()?; // fsync WAL

        // [3] Запись страниц в сегменты (через BufWriter)
        write_pages_grouped_by_segment(self, pages)?;

        // [4] Ротация WAL
        wal.maybe_truncate()?;

        // [5] Обновить last_lsn (в памяти; без write_meta_overwrite)
        self.meta.last_lsn = last_lsn;
        Ok(())
    }

    // ---------- trailer helper (CRC32C or AEAD) ----------

    #[inline]
    fn update_page_trailer(&mut self, page_id: u64, page: &mut [u8], lsn: u64) -> Result<()> {
        if self.tde_enabled {
            let key = self.tde_key_bytes()?; // ensure_tde_key внутри
            page_update_trailer_aead_with(page, key, page_id, lsn)
        } else {
            page_update_checksum(page, self.meta.checksum_kind)
        }
    }
}

// ---------------- helpers (локальные для этого файла) ----------------

fn write_pages_grouped_by_segment(pager: &mut Pager, pages: &mut [(u64, &mut [u8])]) -> Result<()> {
    use std::io::BufWriter;

    let ps_u64 = pager.meta.page_size as u64;

    // seg_no -> Vec<(off_in_seg, idx_in_pages)>
    let mut groups: BTreeMap<u64, Vec<(u64, usize)>> = BTreeMap::new();
    for (idx, (pid, _buf)) in pages.iter().enumerate() {
        let (seg_no, off) = pager.locate(*pid);
        groups.entry(seg_no).or_default().push((off, idx));
    }

    for (seg_no, mut entries) in groups {
        // Отсортируем по off
        entries.sort_unstable_by_key(|e| e.0);

        // Откроем сегмент 1 раз и обернём BufWriter’ом
        let file = pager.open_seg_rw(seg_no, false)?;
        let mut bw = BufWriter::with_capacity(16 * 1024 * 1024, file);

        // Пишем все страницы этого сегмента последовательно
        let mut cur_pos: Option<u64> = None;
        for (off, idx) in entries.into_iter() {
            if cur_pos.map_or(true, |p| p != off) {
                // На всякий случай — сброс буфера перед seek
                let _ = bw.flush();
                bw.seek(SeekFrom::Start(off))?;
            }
            let buf: &[u8] = &pages[idx].1;
            bw.write_all(buf)?;
            cur_pos = Some(off + ps_u64);
        }

        // Завершение: flush + fsync (если включён)
        bw.flush()?;
        if pager.data_fsync {
            let inner = bw.get_ref();
            let _ = inner.sync_all();
        }
    }
    Ok(())
}

fn set_v3_page_lsn_mut(page: &mut [u8], lsn: u64) -> Result<()> {
    if &page[..4] != PAGE_MAGIC {
        return Err(anyhow!("bad page magic"));
    }
    let ptype = LittleEndian::read_u16(&page[OFF_TYPE..OFF_TYPE + 2]);
    match ptype {
        t if t == PAGE_TYPE_KV_RH3 => {
            let mut h = kv_header_read_v3(page)?;
            h.lsn = lsn;
            kv_header_write_v3(page, &h)?;
        }
        t if t == PAGE_TYPE_OVERFLOW3 => {
            let mut h = ovf_header_read_v3(page)?;
            h.lsn = lsn;
            ovf_header_write_v3(page, &h)?;
        }
        other => {
            return Err(anyhow!("set_v3_page_lsn_mut: unsupported page type {}", other));
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn v3_page_lsn(buf: &[u8]) -> Option<u64> {
    if buf.len() < TRAILER_LEN + 64 {
        return None;
    }
    if &buf[..4] != PAGE_MAGIC {
        return None;
    }
    let ptype = LittleEndian::read_u16(&buf[OFF_TYPE..OFF_TYPE + 2]);
    match ptype {
        t if t == PAGE_TYPE_KV_RH3 => Some(LittleEndian::read_u64(&buf[KV_OFF_LSN..KV_OFF_LSN + 8])),
        t if t == PAGE_TYPE_OVERFLOW3 => Some(LittleEndian::read_u64(&buf[OVF_OFF_LSN..OVF_OFF_LSN + 8])),
        _ => None,
    }
}