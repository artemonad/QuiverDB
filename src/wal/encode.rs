//! wal/encode — помощники для кодирования и записи кадров WAL (P2WAL001).
//!
//! Что здесь:
//! - build_hdr_with_crc: построить заголовок записи WAL (28 байт) с рассчитанным CRC32C по
//!   header[0..crc) + payload.
//! - write_record: записать [header][payload] в writer (без seek(End); по текущей позиции).
//!
//! Зависимости:
//! - Константы формата импортируются из супер-модуля (wal/mod.rs).
//! - CRC берётся из crc32c_of_parts (общая утилита wal/mod.rs).

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::io::Write;

use super::{
    crc32c_of_parts, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_FLAGS, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_OFF_RESERVED, WAL_REC_OFF_TYPE,
};

/// Построить заголовок WAL с заполненным CRC32C.
/// CRC считается по header[0..WAL_REC_OFF_CRC32] + payload.
pub fn build_hdr_with_crc(
    rec_type: u8,
    lsn: u64,
    page_id: u64,
    payload: &[u8],
) -> [u8; WAL_REC_HDR_SIZE] {
    let mut hdr = [0u8; WAL_REC_HDR_SIZE];
    hdr[WAL_REC_OFF_TYPE] = rec_type;
    hdr[WAL_REC_OFF_FLAGS] = 0;
    LittleEndian::write_u16(&mut hdr[WAL_REC_OFF_RESERVED..WAL_REC_OFF_RESERVED + 2], 0);
    LittleEndian::write_u64(&mut hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8], lsn);
    LittleEndian::write_u64(
        &mut hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8],
        page_id,
    );
    LittleEndian::write_u32(
        &mut hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4],
        payload.len() as u32,
    );

    let crc = crc32c_of_parts(&hdr[..WAL_REC_OFF_CRC32], payload);
    LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4], crc);
    hdr
}

/// Записать один WAL‑кадр [header][payload] в текущую позицию writer’а.
///
/// Поведение:
/// - Пишет заголовок и payload (если payload не пустой).
/// - Не делает seek(End) — ответственность за позицию лежит на вызывающем коде.
/// - Возвращает ошибку при длине payload > u32::MAX (поле len — u32).
pub fn write_record<W: Write>(
    writer: &mut W,
    rec_type: u8,
    lsn: u64,
    page_id: u64,
    payload: &[u8],
) -> Result<()> {
    // Защита от некорректной длины (формат len — u32)
    if payload.len() > u32::MAX as usize {
        return Err(anyhow!(
            "payload too large for WAL record: {} bytes (max {})",
            payload.len(),
            u32::MAX
        ));
    }

    let hdr = build_hdr_with_crc(rec_type, lsn, page_id, payload);

    // Пишем заголовок и payload по текущей позиции
    writer.write_all(&hdr)?;
    if !payload.is_empty() {
        writer.write_all(payload)?;
    }
    Ok(())
}
