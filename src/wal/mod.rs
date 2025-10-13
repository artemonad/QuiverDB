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
//! - общие утилиты (crc32c_of_parts, write_wal_file_header, wal_path),
//! - re-export публичных типов/функций из подмодулей.

use anyhow::Result;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// -------------------- Публичные константы WAL v2 --------------------

pub const WAL_FILE: &str = "wal-000001.log";
pub const WAL_MAGIC: &[u8; 8] = b"P2WAL001";
pub const WAL_HDR_SIZE: usize = 16; // magic8 + reserved u32 + reserved u32

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

/// Записать заголовок файла WAL (P2WAL001).
/// Публично: используется CLI (cdc-ship) для инициализации sink-файла.
pub fn write_wal_file_header(f: &mut std::fs::File) -> Result<()> {
    f.seek(SeekFrom::Start(0))?;
    f.write_all(WAL_MAGIC)?;
    f.write_all(&[0u8; 4])?; // reserved
    f.write_all(&[0u8; 4])?; // reserved
    Ok(())
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
pub mod writer;
pub mod replay;

// NEW: CDC transport helpers (framing + HMAC-PSK)
pub mod net;

// NEW: персистентное состояние (last_heads_lsn и пр.)
pub mod state;

pub use writer::{Wal, WalGroupCfg};
pub use replay::wal_replay_if_any;