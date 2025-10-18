//! WAL v2 (P2WAL001) — модульная версия
//!
//! Разделение:
//! - writer.rs   — запись WAL (Wal), group-commit (fsync коалессация), метрики.
//! - replay.rs   — реплей WAL (wal_replay_if_any), CRC/LSN-гейтинг и усечение.
//! - registry.rs — реестр/шаринг WalInner и состояния fsync-коалессации между хэндлами.
//! - encode.rs   — помощники по кодированию/записи кадров (заголовок + CRC + запись).
//! - reader.rs   — последовательное чтение кадров WAL с проверкой CRC.
//! - net.rs      — CDC transport helpers (framing + HMAC-PSK).
//! - state.rs    — общие helpers для персистентного состояния CDC/WAL (last_heads_lsn и т.п.). [NEW]
//!
//! В этом модуле (mod.rs) лежат:
//! - публичные константы формата (импортируются снаружи как crate::wal::*),
//! - общие утилиты (crc32c_of_parts, write_wal_file_header, wal_path, stream_id генерация/чтение),
//! - re-export публичных типов/функций из подмодулей.

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// -------------------- Публичные константы WAL v2 --------------------

pub const WAL_FILE: &str = "wal-000001.log";
pub const WAL_MAGIC: &[u8; 8] = b"P2WAL001";
pub const WAL_HDR_SIZE: usize = 16; // magic8 + reserved u64(stream_id)

// Offsets внутри заголовка файла
pub const WAL_HDR_OFF_STREAM_ID: usize = 8; // 8 байт после MAGIC (LE u64)

// Record header (v2): 28 bytes (включая поле CRC на смещении 24..28)
pub const WAL_REC_HDR_SIZE: usize = 28;

// Offsets внутри заголовка записи
pub const WAL_REC_OFF_TYPE: usize = 0;
pub const WAL_REC_OFF_FLAGS: usize = 1;
pub const WAL_REC_OFF_RESERVED: usize = 2;
pub const WAL_REC_OFF_LSN: usize = 4;
pub const WAL_REC_OFF_PAGE_ID: usize = 12;
pub const WAL_REC_OFF_LEN: usize = 20;
pub const WAL_REC_OFF_CRC32: usize = 24;

// Типы записей
pub const WAL_REC_BEGIN: u8 = 1;
pub const WAL_REC_PAGE_IMAGE: u8 = 2;
pub const WAL_REC_PAGE_DELTA: u8 = 3; // зарезервировано — игнорируется реплеем в 2.0
pub const WAL_REC_COMMIT: u8 = 4;
pub const WAL_REC_TRUNCATE: u8 = 5;
// NEW: атомарные обновления голов каталога (между IMAGE… и COMMIT в батче)
pub const WAL_REC_HEADS_UPDATE: u8 = 6;

// Порог ротации (можно вынести в конфиг позднее)
pub const WAL_ROTATE_SIZE: u64 = 8 * 1024 * 1024;

// -------------------- Общие утилиты (видны подмодулям и CLI) --------------------

/// Инкрементальный CRC32C по двум срезам без аллокаций.
///
/// Ранее здесь был вариант с выделением временного буфера и копированием.
/// Это горячий путь — используем crc32c_append, чтобы избежать аллокаций.
#[inline]
pub fn crc32c_of_parts(head_without_crc: &[u8], payload: &[u8]) -> u32 {
    let c = crc32c::crc32c_append(0, head_without_crc);
    crc32c::crc32c_append(c, payload)
}

/// Генерировать случайный stream_id (u64, LE при записи в заголовок).
/// Идempotent в рамках процесса — генерируйте и сохраняйте отдельно (см. wal/state.rs).
pub fn generate_stream_id() -> u64 {
    use rand::RngCore;
    let mut buf = [0u8; 8];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    LittleEndian::read_u64(&buf)
}

/// Записать заголовок файла WAL (P2WAL001) с заданным stream_id.
///
/// Формат 16 байт:
/// - [0..8)  = MAGIC "P2WAL001"
/// - [8..16) = stream_id LE u64 (0 допускается как "не установлен", но нежелателен)
pub fn write_wal_file_header_with_stream_id(f: &mut File, stream_id: u64) -> Result<()> {
    f.seek(SeekFrom::Start(0))?;
    f.write_all(WAL_MAGIC)?;
    let mut sid = [0u8; 8];
    LittleEndian::write_u64(&mut sid, stream_id);
    f.write_all(&sid)?;
    Ok(())
}

/// Старый helper (совместимость): записать заголовок WAL без stream_id.
/// Теперь потоково вызывает write_wal_file_header_with_stream_id(..., 0).
/// В ближайших изменениях запись корректного stream_id будет обеспечена уровнем wal/registry.
pub fn write_wal_file_header(f: &mut File) -> Result<()> {
    write_wal_file_header_with_stream_id(f, 0)
}

/// Прочитать stream_id из заголовка WAL. Возвращает 0, если в файле нули.
/// Ошибка при неправильной магии или слишком коротком файле.
pub fn wal_header_read_stream_id(f: &mut File) -> Result<u64> {
    if f.metadata()?.len() < WAL_HDR_SIZE as u64 {
        anyhow::bail!("wal too small (< header)");
    }
    let mut hdr = [0u8; WAL_HDR_SIZE];
    f.seek(SeekFrom::Start(0))?;
    std::io::Read::read_exact(f, &mut hdr)?;
    if &hdr[..8] != WAL_MAGIC {
        anyhow::bail!("bad WAL magic");
    }
    Ok(LittleEndian::read_u64(
        &hdr[WAL_HDR_OFF_STREAM_ID..WAL_HDR_OFF_STREAM_ID + 8],
    ))
}

/// Построить путь к WAL-файлу для корня БД.
/// Публично: используется CLI (cdc-ship).
pub fn wal_path(root: &Path) -> PathBuf {
    root.join(WAL_FILE)
}

// -------------------- Подмодули и re-export --------------------

// Реестр/шаринг состояния WAL (WalInner, fsync коалессация)
pub mod registry;

// Хелперы для кадрирования/кодирования записей WAL
pub mod encode;

// Чтение кадров WAL с CRC‑валидацией
pub mod reader;

// Основные подсистемы WAL
pub mod replay;
pub mod writer;

// NEW: CDC transport helpers (framing + HMAC-PSK)
pub mod net;

// NEW: персистентное состояние (last_heads_lsn и пр.)
pub mod state;

pub use replay::wal_replay_if_any;
pub use writer::{Wal, WalGroupCfg};
