//! Page format v2: KV with in-page hash table (Robin Hood hashing).
//!
//! Ключевые изменения в этой версии:
//! - Вместо поштучного роста таблицы слотов реализован рост до целевого LF≈0.85.
//! - При изменении capacity выполняется полноценный rebuild таблицы (rehash),
//!   чтобы корректно пересчитать позиции и probe distance.
//!
//! Layout:
//! - Header v2 (64 bytes):
//!   [MAGIC4][ver u16=2][type u16=PAGE_TYPE_KV_RH]
//!   [pad4]
//!   [page_id u64]
//!   [data_start u16]
//!   [table_slots u16]   -- capacity (number of slots at page end)
//!   [used_slots u16]    -- number of occupied slots
//!   [flags u16]
//!   [next_page_id u64]
//!   [lsn u64]           -- page LSN (для реплея WAL)
//!   [seed u64]          -- per-page hash mixing
//!   [crc32 u32]         -- at offset RH_PAGE_CRC_OFF
//!
//! - Data area растёт вперёд от заголовка: запись = [klen u16][vlen u16][key][value]
//! - Hash-таблица растёт назад от конца страницы, слот=4 байта: [off u16][fp u8][dist u8]
//!   off=0xFFFF -> пусто; fp=8-битный fingerprint; dist=probe distance.
//!
//! Операции:
//! - Lookup: линейное пробирование с ранней остановкой (если slot.dist < cur_dist -> miss).
//! - Insert: Robin Hood (swap при dist текущего > dist существующего).
//! - Upsert: при существующем ключе — добавляем новую data-запись и обновляем slot.off.
//! - Delete: back-shift уплотнение кластера и used_slots--.
//!
//! CRC: rh_page_update_crc / rh_page_verify_crc — по всей странице с обнулённым полем CRC.
//!
//! Важно: при изменении размера таблицы мы ОБЯЗАТЕЛЬНО перестраиваем её целиком,
//! т.к. home-индексы меняются (mod capacity), и старые dist становятся некорректными.

use crate::consts::{NO_PAGE, PAGE_HDR_V2_SIZE, PAGE_MAGIC, PAGE_TYPE_KV_RH};
use crate::hash::{hash64, short_fingerprint_u8, HashKind};
use crate::metrics::record_rh_compaction;
use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashSet;

pub const RH_SLOT_SIZE: usize = 4;
pub const RH_PAGE_CRC_OFF: usize = 60; // last 4 bytes of 64-byte header

#[derive(Debug, Clone, Copy)]
pub struct RhPageHeader {
    pub version: u16,       // must be 2
    pub page_type: u16,     // PAGE_TYPE_KV_RH
    pub page_id: u64,
    pub data_start: u16,    // next free byte for data area
    pub table_slots: u16,   // number of slots allocated at page end (capacity)
    pub used_slots: u16,    // number of occupied slots
    pub flags: u16,
    pub next_page_id: u64,
    pub lsn: u64,           // WAL LSN stored on page (используется при реплее)
    pub seed: u64,          // per-page hash mixing
}

#[inline]
fn derive_seed(page_id: u64) -> u64 {
    // Простая детерминированная мешалка.
    let mut x = page_id ^ 0x9E37_79B9_7F4A_7C15u64;
    x ^= x >> 27;
    x = x.wrapping_mul(0x3C79_AC49u64);
    x ^= x >> 33;
    x
}

pub fn rh_header_read(buf: &[u8]) -> Result<RhPageHeader> {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page buffer too small for v2 header"));
    }
    if &buf[..4] != PAGE_MAGIC {
        return Err(anyhow!("bad page magic"));
    }
    let version = LittleEndian::read_u16(&buf[4..6]);
    let page_type = LittleEndian::read_u16(&buf[6..8]);
    if version != 2 || page_type != PAGE_TYPE_KV_RH {
        return Err(anyhow!(
            "not a v2 KV RH page (version={}, type={})",
            version,
            page_type
        ));
    }
    let page_id = LittleEndian::read_u64(&buf[12..20]);
    let data_start = LittleEndian::read_u16(&buf[20..22]);
    let table_slots = LittleEndian::read_u16(&buf[22..24]);
    let used_slots = LittleEndian::read_u16(&buf[24..26]);
    let flags = LittleEndian::read_u16(&buf[26..28]);
    let next_page_id = LittleEndian::read_u64(&buf[28..36]);
    let lsn = LittleEndian::read_u64(&buf[36..44]);
    let seed = LittleEndian::read_u64(&buf[44..52]);
    Ok(RhPageHeader {
        version,
        page_type,
        page_id,
        data_start,
        table_slots,
        used_slots,
        flags,
        next_page_id,
        lsn,
        seed,
    })
}

pub fn rh_header_write(buf: &mut [u8], h: &RhPageHeader) -> Result<()> {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page buffer too small for v2 header"));
    }
    for b in &mut buf[..PAGE_HDR_V2_SIZE] {
        *b = 0;
    }
    buf[..4].copy_from_slice(PAGE_MAGIC);
    LittleEndian::write_u16(&mut buf[4..6], h.version);
    LittleEndian::write_u16(&mut buf[6..8], h.page_type);
    LittleEndian::write_u64(&mut buf[12..20], h.page_id);
    LittleEndian::write_u16(&mut buf[20..22], h.data_start);
    LittleEndian::write_u16(&mut buf[22..24], h.table_slots);
    LittleEndian::write_u16(&mut buf[24..26], h.used_slots);
    LittleEndian::write_u16(&mut buf[26..28], h.flags);
    LittleEndian::write_u64(&mut buf[28..36], h.next_page_id);
    LittleEndian::write_u64(&mut buf[36..44], h.lsn);
    LittleEndian::write_u64(&mut buf[44..52], h.seed);
    // CRC пишется снаружи (rh_page_update_crc)
    Ok(())
}

pub fn rh_page_init(buf: &mut [u8], page_id: u64) -> Result<()> {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page buffer too small"));
    }
    let h = RhPageHeader {
        version: 2,
        page_type: PAGE_TYPE_KV_RH,
        page_id,
        data_start: PAGE_HDR_V2_SIZE as u16,
        table_slots: 0,
        used_slots: 0,
        flags: 0,
        next_page_id: NO_PAGE,
        lsn: 0,
        seed: derive_seed(page_id),
    };
    rh_header_write(buf, &h)
}

pub fn rh_page_is_kv(buf: &[u8]) -> bool {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return false;
    }
    if &buf[..4] != PAGE_MAGIC {
        return false;
    }
    let ver = LittleEndian::read_u16(&buf[4..6]);
    let t = LittleEndian::read_u16(&buf[6..8]);
    ver == 2 && t == PAGE_TYPE_KV_RH
}

// CRC over entire page with zeroed CRC field.
pub fn rh_page_update_crc(buf: &mut [u8]) -> Result<()> {
    use crc32fast::Hasher as Crc32;
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page too small for CRC"));
    }
    let mut hasher = Crc32::new();
    hasher.update(&buf[..RH_PAGE_CRC_OFF]);
    hasher.update(&[0, 0, 0, 0]);
    hasher.update(&buf[RH_PAGE_CRC_OFF + 4..]);
    let crc = hasher.finalize();
    LittleEndian::write_u32(&mut buf[RH_PAGE_CRC_OFF..RH_PAGE_CRC_OFF + 4], crc);
    Ok(())
}

pub fn rh_page_verify_crc(buf: &[u8]) -> Result<bool> {
    use crc32fast::Hasher as Crc32;
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page too small for CRC verify"));
    }
    let stored = LittleEndian::read_u32(&buf[RH_PAGE_CRC_OFF..RH_PAGE_CRC_OFF + 4]);
    if stored == 0 {
        return Ok(true); // accept zero for compatibility
    }
    let mut hasher = Crc32::new();
    hasher.update(&buf[..RH_PAGE_CRC_OFF]);
    hasher.update(&[0, 0, 0, 0]);
    hasher.update(&buf[RH_PAGE_CRC_OFF + 4..]);
    let calc = hasher.finalize();
    Ok(calc == stored)
}

#[inline]
fn table_start(ps: usize, slots: usize) -> usize {
    ps - slots * RH_SLOT_SIZE
}

#[inline]
fn slot_pos(ps: usize, slots: usize, idx: usize) -> usize {
    debug_assert!(idx < slots);
    table_start(ps, slots) + idx * RH_SLOT_SIZE
}

#[inline]
fn is_empty(off: u16) -> bool {
    off == 0xFFFF
}

#[inline]
fn read_slot(buf: &[u8], ps: usize, slots: usize, idx: usize) -> (u16, u8, u8) {
    let p = slot_pos(ps, slots, idx);
    let off = LittleEndian::read_u16(&buf[p..p + 2]);
    let fp = buf[p + 2];
    let dist = buf[p + 3];
    (off, fp, dist)
}

#[inline]
fn write_slot(buf: &mut [u8], ps: usize, slots: usize, idx: usize, off: u16, fp: u8, dist: u8) {
    let p = slot_pos(ps, slots, idx);
    LittleEndian::write_u16(&mut buf[p..p + 2], off);
    buf[p + 2] = fp;
    buf[p + 3] = dist;
}

#[inline]
fn table_bytes(cap: usize) -> usize {
    cap * RH_SLOT_SIZE
}

#[inline]
fn free_end_for_cap(ps: usize, cap: usize) -> usize {
    ps - table_bytes(cap)
}

fn hval_and_fp(kind: HashKind, seed: u64, key: &[u8]) -> (u64, u8) {
    let h = hash64(kind, key) ^ seed;
    (h, short_fingerprint_u8(h))
}

fn read_record<'a>(buf: &'a [u8], off: usize) -> Option<(&'a [u8], &'a [u8])> {
    if off + 4 > buf.len() {
        return None;
    }
    let klen = LittleEndian::read_u16(&buf[off..off + 2]) as usize;
    let vlen = LittleEndian::read_u16(&buf[off + 2..off + 4]) as usize;
    let end = off + 4 + klen + vlen;
    if end > buf.len() {
        return None;
    }
    let k = &buf[off + 4..off + 4 + klen];
    let v = &buf[off + 4 + klen..off + 4 + klen + vlen];
    Some((k, v))
}

fn desired_capacity(need_used: usize) -> usize {
    if need_used == 0 {
        return 0;
    }
    let target_lf = 0.85f32;
    let mut cap = ((need_used as f32) / target_lf).ceil() as usize;
    if cap < 1 {
        cap = 1;
    }
    cap.next_power_of_two()
}

/// Перестроить (rehash) таблицу слотов под новое значение capacity.
fn rebuild_table(buf: &mut [u8], h: &mut RhPageHeader, kind: HashKind, new_cap: usize) -> Result<()> {
    let ps = buf.len();
    let old_cap = h.table_slots as usize;
    let used = h.used_slots as usize;

    if new_cap == old_cap {
        return Ok(());
    }
    if new_cap < used {
        return Err(anyhow!(
            "rebuild_table: new_cap {} < used_slots {}",
            new_cap,
            used
        ));
    }
    if (h.data_start as usize) > free_end_for_cap(ps, new_cap) {
        return Err(anyhow!("rebuild_table: not enough space for table {}", new_cap));
    }

    // Соберём текущие записи (off, hv, fp).
    let mut entries: Vec<(u16, u64, u8)> = Vec::with_capacity(used);
    if old_cap > 0 {
        for i in 0..old_cap {
            let (off, _fp, _dist) = read_slot(buf, ps, old_cap, i);
            if is_empty(off) {
                continue;
            }
            let off_usize = off as usize;
            let (k, _v) = read_record(buf, off_usize)
                .ok_or_else(|| anyhow!("rebuild_table: bad record at off={}", off))?;
            let (hv, fp) = hval_and_fp(kind, h.seed, k);
            entries.push((off, hv, fp));
        }
    }

    // Построим новую таблицу в памяти.
    let mut new_slots: Vec<(u16, u8, u8)> = vec![(0xFFFF, 0, 0); new_cap];

    for (off, hv, fp) in entries {
        let mut idx = (hv % (new_cap as u64)) as usize;
        let mut c_off = off;
        let mut c_fp = fp;
        let mut c_dist: u8 = 0;

        for _ in 0..new_cap {
            let (s_off, s_fp, s_dist) = new_slots[idx];
            if is_empty(s_off) {
                new_slots[idx] = (c_off, c_fp, c_dist);
                break;
            }
            if s_dist < c_dist {
                // Robin Hood swap
                new_slots[idx] = (c_off, c_fp, c_dist);
                c_off = s_off;
                c_fp = s_fp;
                c_dist = s_dist;
            }
            idx += 1;
            if idx == new_cap {
                idx = 0;
            }
            c_dist = c_dist.saturating_add(1);
        }
    }

    // Сбросим новую таблицу в буфер.
    for i in 0..new_cap {
        let (off, fp, dist) = new_slots[i];
        write_slot(buf, ps, new_cap, i, off, fp, dist);
    }

    // Обновим заголовок.
    h.table_slots = new_cap as u16;
    // used_slots остаётся прежним.
    rh_header_write(buf, h)?;
    Ok(())
}

pub fn rh_kv_lookup(buf: &[u8], kind: HashKind, key: &[u8]) -> Result<Option<Vec<u8>>> {
    let h = rh_header_read(buf)?;
    let ps = buf.len();
    if h.table_slots == 0 {
        return Ok(None);
    }
    let (hv, fp) = hval_and_fp(kind, h.seed, key);
    let cap = h.table_slots as usize;
    let mut idx = (hv % (cap as u64)) as usize;
    let mut cur_dist: u8 = 0;

    for _ in 0..cap {
        let (off, sfp, dist) = read_slot(buf, ps, cap, idx);
        if is_empty(off) {
            return Ok(None);
        }
        if dist < cur_dist {
            return Ok(None);
        }
        if sfp == fp {
            if let Some((k, v)) = read_record(buf, off as usize) {
                if k == key {
                    return Ok(Some(v.to_vec()));
                }
            }
        }
        idx += 1;
        if idx == cap {
            idx = 0;
        }
        cur_dist = cur_dist.saturating_add(1);
    }
    Ok(None)
}

/// Вставка/обновление (Robin Hood).
pub fn rh_kv_insert(buf: &mut [u8], kind: HashKind, key: &[u8], val: &[u8]) -> Result<bool> {
    if key.len() > u16::MAX as usize || val.len() > u16::MAX as usize {
        return Err(anyhow!("key/value too long for u16 length"));
    }
    let mut h = rh_header_read(buf)?;
    let ps = buf.len();

    // Поиск существующего ключа
    let mut found_idx: Option<usize> = None;
    let (hv, fp) = hval_and_fp(kind, h.seed, key);
    let mut cap = h.table_slots as usize;
    if cap > 0 {
        let mut idx = (hv % (cap as u64)) as usize;
        let mut cur_dist: u8 = 0;
        for _ in 0..cap {
            let (off, sfp, dist) = read_slot(buf, ps, cap, idx);
            if is_empty(off) {
                break;
            }
            if dist < cur_dist {
                break;
            }
            if sfp == fp {
                if let Some((k, _v)) = read_record(buf, off as usize) {
                    if k == key {
                        found_idx = Some(idx);
                        break;
                    }
                }
            }
            idx += 1;
            if idx == cap {
                idx = 0;
            }
            cur_dist = cur_dist.saturating_add(1);
        }
    }

    let rec_sz = 4 + key.len() + val.len();
    let table_bytes = table_bytes(cap);
    let mut free_end = ps - table_bytes;

    // Обновление существующего ключа: слот не добавляется.
    if let Some(idx) = found_idx {
        if (h.data_start as usize) + rec_sz > free_end {
            return Ok(false); // нет места под данные
        }

        // Запишем новую запись данных
        let off = h.data_start as usize;
        LittleEndian::write_u16(&mut buf[off..off + 2], key.len() as u16);
        LittleEndian::write_u16(&mut buf[off + 2..off + 4], val.len() as u16);
        buf[off + 4..off + 4 + key.len()].copy_from_slice(key);
        buf[off + 4 + key.len()..off + 4 + key.len() + val.len()].copy_from_slice(val);

        // Обновим слот (пересчёт dist относительно текущего cap)
        write_slot(
            buf,
            ps,
            cap,
            idx,
            off as u16,
            fp,
            distance_for_index(cap, hv, idx),
        );

        h.data_start = (off + rec_sz) as u16;
        rh_header_write(buf, &h)?;
        return Ok(true);
    }

    // Новый ключ
    let need_used = (h.used_slots as usize) + 1;
    let mut desired = desired_capacity(need_used).max(cap.max(1)); // не уменьшаем cap здесь

    // Подберём cap, который поместится вместе с записью данных.
    loop {
        let candidate = desired;
        let free_end_candidate = free_end_for_cap(ps, candidate);
        if (h.data_start as usize) + rec_sz <= free_end_candidate && candidate >= need_used {
            if candidate != cap {
                rebuild_table(buf, &mut h, kind, candidate)?;
                cap = candidate;
            }
            break;
        }
        if desired <= 1 {
            return Ok(false);
        }
        desired /= 2;
        if desired < need_used {
            return Ok(false);
        }
    }

    // Пересчитаем free_end после возможного rebuild.
    free_end = free_end_for_cap(ps, cap);
    if (h.data_start as usize) + rec_sz > free_end {
        return Ok(false);
    }

    // Запишем новую запись данных
    let off = h.data_start as usize;
    LittleEndian::write_u16(&mut buf[off..off + 2], key.len() as u16);
    LittleEndian::write_u16(&mut buf[off + 2..off + 4], val.len() as u16);
    buf[off + 4..off + 4 + key.len()].copy_from_slice(key);
    buf[off + 4 + key.len()..off + 4 + key.len() + val.len()].copy_from_slice(val);

    // Вставка в таблицу Robin Hood
    let mut idx = (hv % (cap as u64)) as usize;
    let mut c_off: u16 = off as u16;
    let mut c_fp: u8 = fp;
    let mut c_dist: u8 = 0;

    for _ in 0..cap {
        let (s_off, s_fp, s_dist) = read_slot(buf, ps, cap, idx);
        if is_empty(s_off) {
            write_slot(buf, ps, cap, idx, c_off, c_fp, c_dist);
            h.used_slots = h.used_slots.saturating_add(1);
            h.data_start = (off + rec_sz) as u16;
            rh_header_write(buf, &h)?;
            return Ok(true);
        }
        if s_dist < c_dist {
            // Robin Hood swap
            write_slot(buf, ps, cap, idx, c_off, c_fp, c_dist);
            c_off = s_off;
            c_fp = s_fp;
            c_dist = s_dist;
        }
        idx += 1;
        if idx == cap {
            idx = 0;
        }
        c_dist = c_dist.saturating_add(1);
    }

    Ok(false)
}

#[inline]
fn distance_for_index(cap: usize, hv: u64, idx: usize) -> u8 {
    let home = (hv % (cap as u64)) as usize;
    let dist = if idx >= home {
        idx - home
    } else {
        cap - home + idx
    };
    dist.min(255) as u8
}

pub fn rh_kv_lookup_slot(buf: &[u8], kind: HashKind, key: &[u8]) -> Result<Option<usize>> {
    let h = rh_header_read(buf)?;
    let ps = buf.len();
    if h.table_slots == 0 {
        return Ok(None);
    }
    let (hv, fp) = hval_and_fp(kind, h.seed, key);
    let cap = h.table_slots as usize;
    let mut idx = (hv % (cap as u64)) as usize;
    let mut cur_dist: u8 = 0;

    for _ in 0..cap {
        let (off, sfp, dist) = read_slot(buf, ps, cap, idx);
        if is_empty(off) {
            return Ok(None);
        }
        if dist < cur_dist {
            return Ok(None);
        }
        if sfp == fp {
            if let Some((k, _v)) = read_record(buf, off as usize) {
                if k == key {
                    return Ok(Some(idx));
                }
            }
        }
        idx += 1;
        if idx == cap {
            idx = 0;
        }
        cur_dist = cur_dist.saturating_add(1);
    }
    Ok(None)
}

/// Delete key in-place with back-shift cluster compaction.
pub fn rh_kv_delete_inplace(buf: &mut [u8], kind: HashKind, key: &[u8]) -> Result<bool> {
    let mut h = rh_header_read(buf)?;
    let ps = buf.len();
    let cap = h.table_slots as usize;
    if cap == 0 || h.used_slots == 0 {
        return Ok(false);
    }
    let (hv, _fp) = hval_and_fp(kind, h.seed, key);

    // Find slot index
    let mut idx = (hv % (cap as u64)) as usize;
    let mut cur_dist: u8 = 0;
    let mut found_idx: Option<usize> = None;

    for _ in 0..cap {
        let (off, _fp2, dist) = read_slot(buf, ps, cap, idx);
        if is_empty(off) || dist < cur_dist {
            break;
        }
        if let Some((k, _v)) = read_record(buf, off as usize) {
            if k == key {
                found_idx = Some(idx);
                break;
            }
        }
        idx += 1;
        if idx == cap {
            idx = 0;
        }
        cur_dist = cur_dist.saturating_add(1);
    }

    let Some(mut rm_idx) = found_idx else { return Ok(false) };

    // Remove slot and back-shift cluster
    write_slot(buf, ps, cap, rm_idx, 0xFFFF, 0, 0);

    let mut next = rm_idx + 1;
    if next == cap {
        next = 0;
    }
    loop {
        let (off, fp, dist) = read_slot(buf, ps, cap, next);
        if is_empty(off) || dist == 0 {
            break;
        }
        // Move 'next' to 'rm_idx', decrease dist by 1
        write_slot(buf, ps, cap, rm_idx, off, fp, dist.saturating_sub(1));
        // Clear 'next'
        write_slot(buf, ps, cap, next, 0xFFFF, 0, 0);
        // Advance
        rm_idx = next;
        next += 1;
        if next == cap {
            next = 0;
        }
    }

    h.used_slots = h.used_slots.saturating_sub(1);
    rh_header_write(buf, &h)?;
    Ok(true)
}

pub fn rh_kv_list(buf: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let h = rh_header_read(buf)?;
    let ps = buf.len();
    let cap = h.table_slots as usize;
    let mut out = Vec::with_capacity(h.used_slots as usize);
    for i in 0..cap {
        let (off, _fp, _dist) = read_slot(buf, ps, cap, i);
        if is_empty(off) {
            continue;
        }
        if let Some((k, v)) = read_record(buf, off as usize) {
            out.push((k.to_vec(), v.to_vec()));
        }
    }
    Ok(out)
}

// Полная перепаковка страницы: собираем пары и перезаписываем в новый буфер.
pub fn rh_compact_inplace(buf: &mut [u8], kind: HashKind) -> Result<()> {
    let old = rh_kv_list(buf)?;
    let h0 = rh_header_read(buf)?;
    let ps = buf.len();

    let mut newb = vec![0u8; ps];
    rh_page_init(&mut newb, h0.page_id)?;
    // восстановим next_page_id / lsn / seed
    let mut hn = rh_header_read(&newb)?;
    hn.next_page_id = h0.next_page_id;
    hn.lsn = h0.lsn;
    hn.seed = h0.seed;
    rh_header_write(&mut newb, &hn)?;

    for (k, v) in old {
        if !rh_kv_insert(&mut newb, kind, &k, &v)? {
            return Err(anyhow!("RH page rewrite overflow: not enough space"));
        }
    }
    buf.copy_from_slice(&newb);

    // Метрика: успешная компакция страницы
    record_rh_compaction();

    Ok(())
}

//
// ----- Новый блок: расчёт использования страницы и решение об автокомпакции -----
//

#[derive(Debug, Clone)]
pub struct RhPageUsage {
    pub page_size: usize,
    pub table_slots: usize,
    pub used_slots: usize,
    pub data_start: usize,

    pub table_bytes: usize,
    pub physical_used: usize, // data_start + table_bytes
    pub free_bytes: usize,

    pub live_records: usize,
    pub live_data_bytes: usize, // сумма (4 + klen + vlen) для всех слотов
    pub live_bytes: usize,      // PAGE_HDR_V2_SIZE + table_bytes + live_data_bytes
    pub dead_bytes: usize,      // physical_used - live_bytes
}

/// Быстрый расчёт использования страницы RH.
/// Не падает на частичных повреждениях записей — некорректные записи просто пропускаются.
pub fn rh_page_usage(buf: &[u8]) -> Result<RhPageUsage> {
    let h = rh_header_read(buf)?;
    let ps = buf.len();
    let cap = h.table_slots as usize;
    let used = h.used_slots as usize;
    let data_start = h.data_start as usize;

    let table_bytes = table_bytes(cap);
    let physical_used = data_start + table_bytes;
    let free_bytes = ps.saturating_sub(physical_used);

    let mut live_data_bytes = 0usize;
    let mut live_records = 0usize;
    let mut seen_offs: HashSet<u16> = HashSet::new();

    for i in 0..cap {
        let (off, _fp, _dist) = read_slot(buf, ps, cap, i);
        if is_empty(off) {
            continue;
        }
        if !seen_offs.insert(off) {
            continue;
        }
        let off_usize = off as usize;
        if let Some((k, v)) = read_record(buf, off_usize) {
            live_records += 1;
            live_data_bytes = live_data_bytes.saturating_add(4 + k.len() + v.len());
        }
    }

    let live_bytes = PAGE_HDR_V2_SIZE + table_bytes + live_data_bytes;
    let dead_bytes = physical_used.saturating_sub(live_bytes);

    Ok(RhPageUsage {
        page_size: ps,
        table_slots: cap,
        used_slots: used,
        data_start,
        table_bytes,
        physical_used,
        free_bytes,
        live_records,
        live_data_bytes,
        live_bytes,
        dead_bytes,
    })
}

/// Эвристика: «стоит ли компакть страницу сейчас?»
/// Пороговые значения берутся из env:
/// - P1_RH_COMPACT_DEAD_FRAC (доля мусора от physical_used), по умолчанию 0.30
/// - P1_RH_COMPACT_MIN_BYTES (минимальный объём мусора), по умолчанию 512 байт
pub fn rh_should_compact(buf: &[u8]) -> Result<bool> {
    let usage = rh_page_usage(buf)?;

    let dead_frac = std::env::var("P1_RH_COMPACT_DEAD_FRAC")
        .ok()
        .and_then(|s| s.parse::<f32>().ok())
        .map(|x| x.clamp(0.0, 0.95))
        .unwrap_or(0.30);

    let dead_min = std::env::var("P1_RH_COMPACT_MIN_BYTES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(512);

    if usage.physical_used == 0 {
        return Ok(false);
    }

    let frac_ok = (usage.dead_bytes as f64) >= (usage.physical_used as f64) * (dead_frac as f64);
    let bytes_ok = usage.dead_bytes >= dead_min;

    Ok(frac_ok && bytes_ok)
}