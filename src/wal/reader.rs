//! wal/reader — последовательное чтение кадров WAL (P2WAL001) с проверкой CRC.
//!
//! Stateful API (глобальное состояние удалено):
//! - WalStreamReader хранит локальное состояние prev_was_truncate для пропуска mid‑stream WAL header
//!   строго после TRUNCATE и только в рамках текущего потока.
//! - Глобальная обёртка read_next_record(..) и thread‑local reader удалены.
//!
//! Поведение:
//! - Валидирует CRC32C по header[0..crc) + payload.
//! - Частичный хвост (неполный заголовок/полезная нагрузка) → Ok(None) как EOF.
//! - Mid‑stream WAL header ("P2WAL001" + reserved 8 байт) пропускается ТОЛЬКО если предыдущая запись была TRUNCATE.
//!
//! Использование:
//!   let mut r = WalStreamReader::new();
//!   let mut pos = WAL_HDR_SIZE as u64;
//!   while let Some((rec, next)) = r.read_next(&mut file, pos, file_len)? {
//!       // обработка rec
//!       pos = next;
//!   }

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use super::{
    crc32c_of_parts, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32,
    WAL_REC_OFF_FLAGS, WAL_REC_OFF_LEN, WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_OFF_TYPE,
    WAL_REC_TRUNCATE,
};

/// Одна запись WAL, считанная с диска.
#[derive(Debug)]
pub struct WalRecord {
    pub rec_type: u8,
    pub flags: u8,
    pub lsn: u64,
    pub page_id: u64, // 0 для non‑page (BEGIN/COMMIT/TRUNCATE/HEADS_UPDATE)
    pub payload: Vec<u8>,
    /// Позиция начала заголовка записи (после возможных пропусков mid‑header’ов).
    pub pos: u64,
    /// Общий размер записи (заголовок + payload)
    pub len_total: u64,
}

/// State‑full reader для WAL v2, содержащий внутреннее состояние
/// "предыдущая запись была TRUNCATE" для mid‑header gating.
#[derive(Debug, Default, Clone)]
pub struct WalStreamReader {
    prev_was_truncate: bool,
}

impl WalStreamReader {
    /// Создать новый reader со сброшенным состоянием.
    pub fn new() -> Self {
        Self {
            prev_was_truncate: false,
        }
    }

    /// Сброс состояния (используйте на старте потока после прочтения глобального заголовка WAL).
    pub fn reset_stream(&mut self) {
        self.prev_was_truncate = false;
    }

    /// Считать следующую запись с позиции pos. len — текущая длина файла.
    ///
    /// Возвращает:
    /// - Ok(Some((WalRecord, next_pos))) — запись прочитана;
    /// - Ok(None) — частичный хвост (EOF по трактовке);
    /// - Err(e) — I/O или нарушение целостности (CRC mismatch и т.п.).
    pub fn read_next(
        &mut self,
        f: &mut File,
        mut pos: u64,
        file_len: u64,
    ) -> Result<Option<(WalRecord, u64)>> {
        // Сброс состояния на старте потока (после глобального 16‑байтового заголовка файла)
        if pos == WAL_HDR_SIZE as u64 {
            self.prev_was_truncate = false;
        }

        // Пропуск mid‑stream заголовков разрешён только если предыдущая запись была TRUNCATE.
        loop {
            if pos + (WAL_HDR_SIZE as u64) > file_len {
                break;
            }
            if !self.prev_was_truncate {
                break;
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
                // дочитаем оставшиеся reserved байты заголовка
                let mut reserved = [0u8; WAL_HDR_SIZE - 8];
                if let Err(e) = f.read_exact(&mut reserved) {
                    if e.kind() == std::io::ErrorKind::UnexpectedEof {
                        return Ok(None);
                    }
                    return Err(anyhow!("wal read mid-header rest: {}", e));
                }
                pos += WAL_HDR_SIZE as u64;
                // не меняем prev_was_truncate: следующий реальный кадр его сбросит/установит
                continue;
            } else {
                // не заголовок — возвращаемся к pos и выходим к чтению записи
                f.seek(SeekFrom::Start(pos))?;
                break;
            }
        }

        // Достаточно места под заголовок?
        if pos + (WAL_REC_HDR_SIZE as u64) > file_len {
            return Ok(None);
        }

        // Читаем заголовок
        f.seek(SeekFrom::Start(pos))?;
        let mut rhdr = [0u8; WAL_REC_HDR_SIZE];
        if let Err(e) = f.read_exact(&mut rhdr) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(anyhow!("wal read header: {}", e));
        }

        let payload_len =
            LittleEndian::read_u32(&rhdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
        let total = WAL_REC_HDR_SIZE as u64 + payload_len as u64;
        let next_pos = pos + total;

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

        // CRC32C заголовок[0..crc) + payload
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
        let rec_type = rhdr[WAL_REC_OFF_TYPE];
        let rec = WalRecord {
            rec_type,
            flags: rhdr[WAL_REC_OFF_FLAGS],
            lsn: LittleEndian::read_u64(&rhdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]),
            page_id: LittleEndian::read_u64(&rhdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8]),
            payload,
            pos,
            len_total: total,
        };

        // Обновим локальное состояние
        self.prev_was_truncate = rec_type == WAL_REC_TRUNCATE;

        Ok(Some((rec, next_pos)))
    }
}
