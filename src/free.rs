//! Free-list of pages (v0.6).
//!
//! Файл <root>/free:
//! Header (24 B):
//!   [magic8="P1FREE01"][ver u32=1][count u32][reserved u64]
//! Tail:
//!   последовательность u64 page_id (LE), по одному на запись.
//!
//! Устойчивость:
//! - Источник истины для количества — длина файла: (len - FREE_HDR_SIZE) / 8.
//!   Поле count поддерживается best-effort (обновляется после успешной операции),
//!   но не используется при чтении.

use anyhow::{anyhow, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::consts::{FREE_FILE, FREE_HDR_SIZE, FREE_MAGIC};

// Внутренние смещения полей в заголовке
const OFF_COUNT: u64 = 12;    // u32

const FREE_VER_1: u32 = 1;

pub struct FreeList {
    pub path: PathBuf,
}

impl FreeList {
    /// Создать новый пустой free-файл. Ошибка, если уже существует.
    pub fn create(root: &Path) -> Result<Self> {
        let path = root.join(FREE_FILE);
        if path.exists() {
            return Err(anyhow!("free list already exists at {}", path.display()));
        }
        let mut f = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("create free {}", path.display()))?;

        // header
        f.write_all(FREE_MAGIC)?;
        f.write_u32::<LittleEndian>(FREE_VER_1)?;
        f.write_u32::<LittleEndian>(0)?; // count = 0
        f.write_u64::<LittleEndian>(0)?; // reserved
        f.sync_all()?;

        Ok(Self { path })
    }

    /// Открыть существующий free-файл и проверить заголовок.
    pub fn open(root: &Path) -> Result<Self> {
        let path = root.join(FREE_FILE);
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open free {}", path.display()))?;

        // validate header
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        if &magic != FREE_MAGIC {
            return Err(anyhow!("bad FREE magic in {}", path.display()));
        }
        let ver = f.read_u32::<LittleEndian>()?;
        if ver != FREE_VER_1 {
            return Err(anyhow!(
                "unsupported FREE version {} in {}",
                ver,
                path.display()
            ));
        }
        // count (u32)
        let _count = f.read_u32::<LittleEndian>()?;
        let _reserved = f.read_u64::<LittleEndian>()?;

        Ok(Self { path })
    }

    /// Текущее число свободных страниц (по длине файла).
    pub fn count(&self) -> Result<u64> {
        let len = std::fs::metadata(&self.path)?.len();
        if len < FREE_HDR_SIZE as u64 {
            return Err(anyhow!(
                "free file too small (< header): {}",
                self.path.display()
            ));
        }
        Ok((len - FREE_HDR_SIZE as u64) / 8)
    }

    /// Добавить свободную страницу в конец списка.
    pub fn push(&self, page_id: u64) -> Result<()> {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .with_context(|| format!("open free for push {}", self.path.display()))?;

        // append page_id
        f.seek(SeekFrom::End(0))?;
        f.write_u64::<LittleEndian>(page_id)?;
        f.sync_all()?;

        // update header.count best-effort
        update_header_count(&mut f)?;

        Ok(())
    }

    /// Вытянуть последнюю свободную страницу. None, если список пуст.
    pub fn pop(&self) -> Result<Option<u64>> {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .with_context(|| format!("open free for pop {}", self.path.display()))?;

        let len = f.metadata()?.len();
        if len < FREE_HDR_SIZE as u64 {
            return Err(anyhow!(
                "free file too small (< header): {}",
                self.path.display()
            ));
        }
        if len == FREE_HDR_SIZE as u64 {
            return Ok(None);
        }

        // read last id
        let last_off = len - 8;
        f.seek(SeekFrom::Start(last_off))?;
        let pid = f.read_u64::<LittleEndian>()?;

        // truncate last entry
        f.set_len(last_off)?;
        f.sync_all()?;

        // update header.count best-effort
        update_header_count(&mut f)?;

        Ok(Some(pid))
    }

    /// Путь к free-файлу.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Обновить поле count в заголовке (best-effort).
fn update_header_count(f: &mut std::fs::File) -> Result<()> {
    let len = f.metadata()?.len();
    let cnt = if len >= FREE_HDR_SIZE as u64 {
        ((len - FREE_HDR_SIZE as u64) / 8) as u64
    } else {
        0
    };
    let cnt_u32 = cnt.min(u32::MAX as u64) as u32;

    // Пишем строго в поле count (offset = 12), не затирая version!
    f.seek(SeekFrom::Start(OFF_COUNT))?;
    f.write_u32::<LittleEndian>(cnt_u32)?;
    f.sync_all()?;
    Ok(())
}