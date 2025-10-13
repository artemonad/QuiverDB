use byteorder::{ByteOrder, LittleEndian};

use crate::page::common::{
    TRAILER_LEN, KV_HDR_MIN, KV_SLOT_SIZE, KV_EMPTY_OFF,
};
use super::header::{KvHeaderV3, kv_header_read_v3};

/// Прочитать запись данных по смещению `off`.
/// Формат: [klen u16][vlen u32][expires_at_sec u32][vflags u8][key][value].
/// Возвращает None при выходе за пределы страницы.
///
/// ВНИМАНИЕ: этот хелпер не проверяет границы data‑area (до slot‑таблицы). Для безопасного
/// обхода packed‑страниц используйте kv_find_record_by_key/kv_for_each_record, которые учитывают
/// data_end и не читают за пределы области данных.
pub fn kv_read_record<'a>(
    page: &'a [u8],
    off: usize,
) -> Option<(&'a [u8], &'a [u8], u32 /*expires_at_sec*/, u8 /*vflags*/)> {
    // klen + vlen + expires + vflags занимают 2 + 4 + 4 + 1 = 11 байт
    if off + 11 > page.len() {
        return None;
    }
    let klen = LittleEndian::read_u16(&page[off..off + 2]) as usize;
    let vlen = LittleEndian::read_u32(&page[off + 2..off + 6]) as usize;
    let expires_at_sec = LittleEndian::read_u32(&page[off + 6..off + 10]);
    let vflags = page[off + 10];

    let base = off + 11;
    let end = base + klen + vlen;
    if end > page.len() {
        return None;
    }
    let key = &page[base..base + klen];
    let val = &page[base + klen..base + klen + vlen];
    Some((key, val, expires_at_sec, vflags))
}

// ------------------------------------------------------------------------------------
// Helpers for KV‑packing (slot table). Backward compatible with single-record.
// ------------------------------------------------------------------------------------

/// Вычислить верхнюю границу data‑area (data_end).
/// - Если table_slots==0 → data_end = ps - TRAILER_LEN (нет slot‑таблицы).
/// - Если slots>0 → data_end = ps - TRAILER_LEN - slots*KV_SLOT_SIZE.
#[inline]
fn data_end_for_page(hdr: &KvHeaderV3, ps: usize) -> Option<usize> {
    if hdr.table_slots == 0 {
        ps.checked_sub(TRAILER_LEN)
    } else {
        ps.checked_sub(TRAILER_LEN + (hdr.table_slots as usize) * KV_SLOT_SIZE)
    }
}

/// Лёгкий 1‑байтовый fingerprint ключа (xxhash64(seed=0) low‑8).
/// Используется для раннего отсева слотов перед чтением записи.
/// Замечание: значение 0 допустимо; в слотах старых страниц fp=0 трактуется как “без отпечатка”.
pub fn kv_fp8(key: &[u8]) -> u8 {
    use std::hash::Hasher;
    let mut h = twox_hash::XxHash64::with_seed(0);
    h.write(key);
    (h.finish() & 0xFF) as u8
}

/// Попытка найти запись по ключу, используя слот‑таблицу (если есть).
/// Fallback: если table_slots==0, читает одиночную запись от KV_HDR_MIN и сравнивает ключ.
/// Безопасно: учитывает границы data‑area (data_end), не читает за пределы.
///
/// Порядок обхода слотов: ОБРАТНЫЙ (новые → старые), чтобы внутри одной страницы
/// при наличии нескольких версий ключа побеждала самая новая (tail‑wins).
///
/// Ускорение:
/// - Если в слоте fp!=0 и он не совпадает с kv_fp8(key), слот пропускается без чтения записи.
/// - Если fp==0 (старые страницы/слоты без отпечатка) — проверка отпечатка игнорируется.
pub fn kv_find_record_by_key<'a>(
    page: &'a [u8],
    key: &[u8],
) -> Option<(&'a [u8], &'a [u8], u32 /*expires_at_sec*/, u8 /*vflags*/)> {
    let hdr = kv_header_read_v3(page).ok()?;
    let ps = page.len();

    // Верхняя граница допустимых данных (до slot‑таблицы)
    let data_end = data_end_for_page(&hdr, ps)?;

    if hdr.table_slots == 0 {
        // Одиночная запись — проверим, что помещается и что ключ совпадает.
        if let Some((k, v, e, f)) = read_record_at_checked(page, KV_HDR_MIN, data_end) {
            if k == key {
                return Some((k, v, e, f));
            }
        }
        return None;
    }

    // Слот‑таблица размещена у хвоста страницы
    let table_slots = hdr.table_slots as usize;
    let table_start = ps.checked_sub(TRAILER_LEN + table_slots * KV_SLOT_SIZE)?;

    // Вычислим fp искомого key один раз
    let want_fp = kv_fp8(key);

    // Обходим слоты в обратном порядке (новее → старее)
    use std::collections::HashSet;
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

        // Fingerprint из слота (0 — означает «нет отпечатка», не фильтруем по нему)
        let fp_in_slot = page[slot_off + 4];
        if fp_in_slot != 0 && fp_in_slot != want_fp {
            continue;
        }

        let off_usize = off as usize;
        if let Some((k, v, e, f)) = read_record_at_checked(page, off_usize, data_end) {
            if k == key {
                return Some((k, v, e, f));
            }
        }
    }

    // На всякий случай проверим одиночную запись (совместимость со старыми страницами),
    // но только если ключ совпадает.
    if let Some((k, v, e, f)) = read_record_at_checked(page, KV_HDR_MIN, data_end) {
        if k == key {
            return Some((k, v, e, f));
        }
    }

    None
}

/// Обойти все записи на странице (для сканов/построения индексов).
/// Безопасно: учитывает data_end.
/// Порядок: ОБРАТНЫЙ порядок слотов (новые → старые); одиночная запись — как есть.
pub fn kv_for_each_record<'a, F>(page: &'a [u8], mut f: F)
where
    F: FnMut(&'a [u8], &'a [u8], u32, u8),
{
    let hdr = match kv_header_read_v3(page) {
        Ok(h) => h,
        Err(_) => return,
    };
    let ps = page.len();
    let Some(data_end) = data_end_for_page(&hdr, ps) else {
        return;
    };

    if hdr.table_slots == 0 {
        if let Some((k, v, e, fl)) = read_record_at_checked(page, KV_HDR_MIN, data_end) {
            f(k, v, e, fl);
        }
        return;
    }

    use std::collections::HashSet;
    let table_slots = hdr.table_slots as usize;
    let table_start = match ps.checked_sub(TRAILER_LEN + table_slots * KV_SLOT_SIZE) {
        Some(v) => v,
        None => return,
    };

    let mut seen_off: HashSet<u32> = HashSet::new();

    // Итерируем слоты в обратном порядке
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

/// Прочитать запись по произвольному смещению off с учётом верхней границы data_end.
/// Возвращает None, если запись целиком не помещается в data‑area.
#[inline]
fn read_record_at_checked<'a>(
    page: &'a [u8],
    off: usize,
    data_end: usize,
) -> Option<(&'a [u8], &'a [u8], u32, u8)> {
    // Минимальная “шапка” записи
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