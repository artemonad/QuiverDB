//! wal/state — персистентное состояние для CDC/WAL.
//!
//! Назначение:
//! - Хранение маркеров консистентности, используемых CDC apply/replay и инструментами:
//!   * last_heads_lsn — LSN последнего применённого HEADS_UPDATE.
//!   * last_seq       — последний принятый seq для tcp+psk кадров.
//!
//! Формат файлов (LE, 8 байт):
//! - <root>/.heads_lsn.bin : [u64 last_heads_lsn]
//! - <root>/.cdc_seq.bin   : [u64 last_seq]
//!
//! Поведение:
//! - Если файл отсутствует — load_* возвращает 0.
//! - Запись: truncate + write 8 байт + sync_all() (best-effort).
//!
//! Замечание:
//! - Хранилище намеренно простое (один u64). Этого достаточно для LSN/seq-гейтинга,
//!   заметно упрощает использование из CLI/реплея без мета-миграций.

use anyhow::{Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const HEADS_LSN_FILE: &str = ".heads_lsn.bin";
pub const CDC_SEQ_FILE: &str = ".cdc_seq.bin";

#[inline]
pub fn heads_lsn_path(root: &Path) -> PathBuf {
    root.join(HEADS_LSN_FILE)
}

#[inline]
pub fn seq_path(root: &Path) -> PathBuf {
    root.join(CDC_SEQ_FILE)
}

/// Загрузить last_heads_lsn (0, если файл отсутствует).
pub fn load_last_heads_lsn(root: &Path) -> Result<u64> {
    let p = heads_lsn_path(root);
    if !p.exists() {
        return Ok(0);
    }
    let mut f = OpenOptions::new()
        .read(true)
        .open(&p)
        .with_context(|| format!("open {}", p.display()))?;
    let mut buf = [0u8; 8];
    f.read_exact(&mut buf)?;
    Ok(LittleEndian::read_u64(&buf))
}

/// Сохранить last_heads_lsn в файл.
pub fn store_last_heads_lsn(root: &Path, v: u64) -> Result<()> {
    let p = heads_lsn_path(root);
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&p)
        .with_context(|| format!("open {} for write", p.display()))?;
    let mut buf = [0u8; 8];
    LittleEndian::write_u64(&mut buf, v);
    f.write_all(&buf)?;
    let _ = f.sync_all();
    Ok(())
}

/// Загрузить last_seq (0, если файл отсутствует).
pub fn load_last_seq(root: &Path) -> Result<u64> {
    let p = seq_path(root);
    if !p.exists() {
        return Ok(0);
    }
    let mut f = OpenOptions::new()
        .read(true)
        .open(&p)
        .with_context(|| format!("open {}", p.display()))?;
    let mut buf = [0u8; 8];
    f.read_exact(&mut buf)?;
    Ok(LittleEndian::read_u64(&buf))
}

/// Сохранить last_seq в файл.
pub fn store_last_seq(root: &Path, v: u64) -> Result<()> {
    let p = seq_path(root);
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&p)
        .with_context(|| format!("open {} for write", p.display()))?;
    let mut buf = [0u8; 8];
    LittleEndian::write_u64(&mut buf, v);
    f.write_all(&buf)?;
    let _ = f.sync_all();
    Ok(())
}