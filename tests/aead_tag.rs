use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};

use QuiverDB::page::{
    kv_init_v3, kv_header_read_v3, kv_header_write_v3,
    page_update_trailer_aead_with, page_verify_trailer_aead_with,
    KV_HDR_MIN, TRAILER_LEN,
};

/// Простой writer одной KV‑записи: [klen u16][vlen u32][expires u32][vflags u8][key][value].
fn write_single_record_kv(page: &mut [u8], key: &[u8], value: &[u8], expires_at_sec: u32, vflags: u8) -> Result<()> {
    if page.len() < KV_HDR_MIN + TRAILER_LEN {
        anyhow::bail!("page too small for KV record");
    }
    let off = KV_HDR_MIN;
    let need = off + 2 + 4 + 4 + 1 + key.len() + value.len();
    if need > page.len() - TRAILER_LEN {
        anyhow::bail!("record does not fit in page");
    }
    LittleEndian::write_u16(&mut page[off..off + 2], key.len() as u16);
    LittleEndian::write_u32(&mut page[off + 2..off + 6], value.len() as u32);
    LittleEndian::write_u32(&mut page[off + 6..off + 10], expires_at_sec);
    page[off + 10] = vflags;
    let base = off + 11;
    page[base..base + key.len()].copy_from_slice(key);
    page[base + key.len()..base + key.len() + value.len()].copy_from_slice(value);

    // Обновим data_start
    let mut h = kv_header_read_v3(page)?;
    h.data_start = (base + key.len() + value.len()) as u32;
    kv_header_write_v3(page, &h)?;
    Ok(())
}

/// Проверка вычисления/проверки AEAD‑тега (integrity‑only).
/// 1) Формируем KV‑страницу, проставляем LSN/next_pid, считаем тег — verify должен пройти.
/// 2) Модифицируем байт данных — verify должен упасть.
/// 3) Зануляем трейлер — verify должен вернуть false.
#[test]
fn aead_tag_compute_and_verify() -> Result<()> {
    // Геометрия страницы
    let ps = 64 * 1024;
    let mut page = vec![0u8; ps];

    // Идентификаторы
    let page_id: u64 = 42;
    let lsn: u64 = 777;

    // Инициализация KV‑страницы
    kv_init_v3(&mut page, page_id, 0)?;
    write_single_record_kv(&mut page, b"k", b"v", 0, 0)?;
    {
        let mut h = kv_header_read_v3(&page)?;
        h.next_page_id = u64::MAX;
        // Для согласованности выставим lsn в заголовке (nonce всё равно берётся из аргумента)
        h.lsn = lsn;
        kv_header_write_v3(&mut page, &h)?;
    }

    // Синтетический ключ 32 байта
    let key = [0x22u8; 32];

    // 1) Вычисление и проверка тега
    page_update_trailer_aead_with(&mut page, &key, page_id, lsn)?;
    assert!(
        page_verify_trailer_aead_with(&page, &key, page_id, lsn)?,
        "AEAD tag must verify on unmodified page"
    );

    // 2) Модифицируем один байт значения и проверим, что verify падает
    // Смещаемся к началу “value”: KV_HDR_MIN + 11 + klen
    let klen = 1usize; // "k"
    let base = KV_HDR_MIN + 11 + klen;
    page[base] ^= 0x01; // flip 1 bit
    assert!(
        !page_verify_trailer_aead_with(&page, &key, page_id, lsn)?,
        "AEAD tag verify must fail after data modification"
    );
    // Вернём назад байт
    page[base] ^= 0x01;

    // 3) Нулевой трейлер должен считаться невалидным
    let ps_len = page.len();
    for b in &mut page[ps_len - TRAILER_LEN..ps_len] {
        *b = 0;
    }
    assert!(
        !page_verify_trailer_aead_with(&page, &key, page_id, lsn)?,
        "zero trailer must be invalid in AEAD mode"
    );

    Ok(())
}