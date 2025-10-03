//! Операции с KV на RH‑странице: lookup/insert/delete/list/compact,
//! а также расчёт использования страницы и эвристика автокомпакции.

use anyhow::{anyhow, Result};
use std::collections::HashSet;

use crate::consts::PAGE_HDR_V2_SIZE;
use crate::hash::HashKind;
use crate::metrics::record_rh_compaction;
use byteorder::{ByteOrder, LittleEndian}; // <-- ДОБАВЛЕНО

use super::header::{rh_header_read, rh_header_write, rh_page_init, RhPageHeader};
use super::table::{
    desired_capacity, distance_for_index, free_end_for_cap, hval_and_fp, is_empty, read_record,
    read_slot, rebuild_table, table_bytes, write_slot,
};

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
    let table_bytes_now = table_bytes(cap);
    let mut free_end = ps - table_bytes_now;

    // Обновление существующего ключа (слот не добавляем)
    if let Some(idx) = found_idx {
        if (h.data_start as usize) + rec_sz > free_end {
            return Ok(false);
        }

        let off = h.data_start as usize;
        LittleEndian::write_u16(&mut buf[off..off + 2], key.len() as u16);
        LittleEndian::write_u16(&mut buf[off + 2..off + 4], val.len() as u16);
        buf[off + 4..off + 4 + key.len()].copy_from_slice(key);
        buf[off + 4 + key.len()..off + 4 + key.len() + val.len()].copy_from_slice(val);

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

    // Новый ключ — возможно расширение таблицы
    let need_used = (h.used_slots as usize) + 1;
    let mut desired = desired_capacity(need_used).max(cap.max(1));

    // Подыщем подходящую ёмкость (сверху вниз), чтобы влезли и данные, и таблица
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

    // Обновим free_end после (возможного) rebuild
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

pub fn rh_kv_delete_inplace(buf: &mut [u8], kind: HashKind, key: &[u8]) -> Result<bool> {
    let mut h = rh_header_read(buf)?;
    let ps = buf.len();
    let cap = h.table_slots as usize;
    if cap == 0 || h.used_slots == 0 {
        return Ok(false);
    }
    let (hv, _fp) = hval_and_fp(kind, h.seed, key);

    // Найдём индекс слота ключа
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

    // Удаляем слот и back-shift уплотняем кластер
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
        write_slot(buf, ps, cap, rm_idx, off, fp, dist.saturating_sub(1));
        write_slot(buf, ps, cap, next, 0xFFFF, 0, 0);
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

/// Полная перепаковка страницы: собираем пары и перезаписываем в новый буфер.
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

    // Метрика
    record_rh_compaction();
    Ok(())
}

//
// ----- Статистика использования и эвристика автокомпакции -----
//

#[derive(Debug, Clone)]
pub struct RhPageUsage {
    pub page_size: usize,
    pub table_slots: usize,
    pub used_slots: usize,
    pub data_start: usize,

    pub table_bytes: usize,
    pub physical_used: usize,
    pub free_bytes: usize,

    pub live_records: usize,
    pub live_data_bytes: usize,
    pub live_bytes: usize,
    pub dead_bytes: usize,
}

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
        if let Some((k, v)) = read_record(buf, off as usize) {
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

    let frac_ok =
        (usage.dead_bytes as f64) >= (usage.physical_used as f64) * (dead_frac as f64);
    let bytes_ok = usage.dead_bytes >= dead_min;

    Ok(frac_ok && bytes_ok)
}