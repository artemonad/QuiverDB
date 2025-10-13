//! wal/reader — последовательное чтение кадров WAL (P2WAL001) с проверкой CRC.
//!
//! Назначение:
//! - Обеспечить единый, протестируемый путь чтения записей из WAL для replay/CDC.
//! - Валидирует CRC32C по header[0..crc) + payload.
//! - Толерантен к частичному хвосту (возвращает Ok(None)).
//! - NEW: толерантен к повторному 16‑байтовому глобальному заголовку WAL в середине потока
//!        (после TRUNCATE производитель может повторить "P2WAL001"+reserved). Такие заголовки
//!        пропускаются прозрачно.
//!
//! Использование:
//!   let len = file.metadata()?.len();
//!   let mut pos = WAL_HDR_SIZE as u64;
//!   loop {
//!       match read_next_record(&mut file, pos, len)? {
//!           Some((rec, next)) => { /* обработка rec */ pos = next; }
//!           None => break, // EOF или частичный хвост
//!       }
//!   }

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::io::{Read, Seek, SeekFrom};

use super::{
    WAL_REC_HDR_SIZE,
    WAL_REC_OFF_TYPE, WAL_REC_OFF_FLAGS,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_OFF_LEN, WAL_REC_OFF_CRC32,
    crc32c_of_parts,
    // NEW: для пропуска mid-stream заголовка
    WAL_MAGIC, WAL_HDR_SIZE,
};

/// Одна запись WAL, считанная с диска.
#[derive(Debug)]
pub struct WalRecord {
    pub rec_type: u8,
    pub flags: u8,
    pub lsn: u64,
    pub page_id: u64, // 0 для non‑page (BEGIN/COMMIT/TRUNCATE/HEADS_UPDATE)
    pub payload: Vec<u8>,
    /// Позиция начала заголовка записи (после пропусков mid‑stream header’ов).
    pub pos: u64,
    /// Общий размер записи (заголовок + payload)
    pub len_total: u64,
}

/// Считать следующую запись с позиции pos. len — текущая длина файла.
///
/// Поведение:
/// - Прозрачно пропускает один или несколько подряд идущих 16‑байтовых глобальных заголовков
///   WAL (“P2WAL001” + 8 reserved), если они встречаются mid‑stream.
/// - Возвращает:
///   * Ok(Some((WalRecord, next_pos))) — запись прочитана и валидна, next_pos = pos' + total,
///     где pos' — позиция первого байта заголовка записи (после возможных пропусков mid‑header).
///   * Ok(None) — частичный хвост (заголовок или payload не умещаются в len) — лёгкий EOF.
///   * Err(e) — нарушение целостности (CRC mismatch) или I/O ошибка.
pub fn read_next_record(
    f: &mut std::fs::File,
    mut pos: u64,
    file_len: u64,
) -> Result<Option<(WalRecord, u64)>> {
    // Пропустим все подряд идущие mid‑stream заголовки WAL (если они присутствуют).
    // Безопасно: заголовок записи начинается с type (u8), который не может совпасть с 'P'.
    loop {
        // Достаточно места под возможный mid‑header?
        if pos + (WAL_HDR_SIZE as u64) > file_len {
            break; // недостаточно для заголовка — выходим к попытке чтения записи
        }

        // Проверим magic на текущей позиции
        f.seek(SeekFrom::Start(pos))?;
        let mut magic8 = [0u8; 8];
        if let Err(e) = f.read_exact(&mut magic8) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(anyhow!("wal read mid-header magic: {}", e));
        }

        if &magic8 == WAL_MAGIC {
            // Есть полный mid‑header? (WAL_HDR_SIZE = 16)
            // Мы уже прочитали 8 байт; дочитаем оставшиеся 8, затем пропустим его.
            let mut reserved = [0u8; WAL_HDR_SIZE - 8];
            if let Err(e) = f.read_exact(&mut reserved) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                }
                return Err(anyhow!("wal read mid-header rest: {}", e));
            }
            pos += WAL_HDR_SIZE as u64;
            // И продолжаем цикл — возможно подряд несколько заголовков.
            continue;
        } else {
            // Не похоже на заголовок WAL — вернёмся к pos и перейдём к чтению записи
            // (файл уже сдвинут на 8 байт — вернём курсор).
            f.seek(SeekFrom::Start(pos))?;
            break;
        }
    }

    // Достаточно места под заголовок записи?
    if pos + (WAL_REC_HDR_SIZE as u64) > file_len {
        return Ok(None);
    }

    // Читаем заголовок записи
    f.seek(SeekFrom::Start(pos))?;
    let mut rhdr = [0u8; WAL_REC_HDR_SIZE];
    if let Err(e) = f.read_exact(&mut rhdr) {
        // частичный заголовок — считаем хвостом
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(anyhow!("wal read header: {}", e));
    }

    // Длина payload
    let payload_len = LittleEndian::read_u32(&rhdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
    let total = WAL_REC_HDR_SIZE as u64 + payload_len as u64;
    let next_pos = pos + total;

    // Достаточно ли файла под payload?
    if next_pos > file_len {
        return Ok(None);
    }

    // Читаем payload
    let mut payload = vec![0u8; payload_len];
    if let Err(e) = f.read_exact(&mut payload) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(anyhow!("wal read payload: {}", e));
    }

    // CRC32C
    let stored_crc = LittleEndian::read_u32(&rhdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4]);
    let calc_crc = crc32c_of_parts(&rhdr[..WAL_REC_OFF_CRC32], &payload);
    if stored_crc != calc_crc {
        return Err(anyhow!(
            "WAL CRC mismatch at pos {} (stored={}, calc={})",
            pos,
            stored_crc,
            calc_crc
        ));
    }

    // Сформируем запись
    let rec = WalRecord {
        rec_type: rhdr[WAL_REC_OFF_TYPE],
        flags: rhdr[WAL_REC_OFF_FLAGS],
        lsn: LittleEndian::read_u64(&rhdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]),
        page_id: LittleEndian::read_u64(&rhdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8]),
        payload,
        pos,
        len_total: total,
    };

    Ok(Some((rec, next_pos)))
}