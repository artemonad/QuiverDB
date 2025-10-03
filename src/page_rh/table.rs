//! Слой таблицы слотов Robin Hood и утилиты для работы с данными на странице.

use crate::hash::{hash64, short_fingerprint_u8, HashKind};
use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};

use super::header::{rh_header_write, RhPageHeader};

/// Размер слота таблицы: [off u16][fp u8][dist u8]
pub const RH_SLOT_SIZE: usize = 4;

#[inline]
pub fn table_start(ps: usize, slots: usize) -> usize {
    ps - slots * RH_SLOT_SIZE
}

#[inline]
pub fn slot_pos(ps: usize, slots: usize, idx: usize) -> usize {
    debug_assert!(idx < slots);
    table_start(ps, slots) + idx * RH_SLOT_SIZE
}

#[inline]
pub fn is_empty(off: u16) -> bool {
    off == 0xFFFF
}

#[inline]
pub fn read_slot(buf: &[u8], ps: usize, slots: usize, idx: usize) -> (u16, u8, u8) {
    let p = slot_pos(ps, slots, idx);
    let off = LittleEndian::read_u16(&buf[p..p + 2]);
    let fp = buf[p + 2];
    let dist = buf[p + 3];
    (off, fp, dist)
}

#[inline]
pub fn write_slot(buf: &mut [u8], ps: usize, slots: usize, idx: usize, off: u16, fp: u8, dist: u8) {
    let p = slot_pos(ps, slots, idx);
    LittleEndian::write_u16(&mut buf[p..p + 2], off);
    buf[p + 2] = fp;
    buf[p + 3] = dist;
}

#[inline]
pub fn table_bytes(cap: usize) -> usize {
    cap * RH_SLOT_SIZE
}

#[inline]
pub fn free_end_for_cap(ps: usize, cap: usize) -> usize {
    ps - table_bytes(cap)
}

/// Хэш и 8-битный отпечаток для быстрого фильтра при сравнении ключей.
#[inline]
pub fn hval_and_fp(kind: HashKind, seed: u64, key: &[u8]) -> (u64, u8) {
    let h = hash64(kind, key) ^ seed;
    (h, short_fingerprint_u8(h))
}

/// Чтение записи данных по смещению: [klen u16][vlen u16][key][value]
#[inline]
pub fn read_record<'a>(buf: &'a [u8], off: usize) -> Option<(&'a [u8], &'a [u8])> {
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

/// Желаемая ёмкость таблицы для числа занятых слотов при целевом LF≈0.85.
#[inline]
pub fn desired_capacity(need_used: usize) -> usize {
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

/// Перестроение (rehash) таблицы под новое значение capacity.
/// Требования:
/// - new_cap >= used_slots
/// - data_start <= free_end(new_cap)
pub fn rebuild_table(buf: &mut [u8], h: &mut RhPageHeader, kind: HashKind, new_cap: usize) -> Result<()> {
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

/// Расстояние (probe distance) для записи с хэшем hv, если она находится в индексе idx.
#[inline]
pub fn distance_for_index(cap: usize, hv: u64, idx: usize) -> u8 {
    let home = (hv % (cap as u64)) as usize;
    let dist = if idx >= home {
        idx - home
    } else {
        cap - home + idx
    };
    dist.min(255) as u8
}