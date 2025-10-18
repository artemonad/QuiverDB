//! free — минимальный free‑лист 2.0 (файл `<root>/free`).
//!
//! Формат (LE):
//! - Header (16 B):
//!   [magic8="P2FREE01"][ver u32=1][reserved u32=0]
//! - Tail:
//!   последовательность u64 page_id (LE), по одному на запись.
//!
//! Политика:
//! - Источник истины для количества — длина файла: (len - HDR) / 8.
//! - Операции push/pop обновляют длину и fsync’ят файл (best-effort).
//!
//! Примечание:
//! - Это простой, однопоточный в терминах процесса API. Вызовы должны
//!   выполняться под внешней синхронизацией на уровне Db/Pager.

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const FREE_FILE: &str = "free";
const FREE_MAGIC: &[u8; 8] = b"P2FREE01";
const FREE_VER: u32 = 1;
const FREE_HDR_SIZE: u64 = 16;

pub struct FreeList {
    path: PathBuf,
}

impl FreeList {
    /// Создать новый пустой free‑лист. Ошибка, если уже существует.
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
        let mut buf4 = [0u8; 4];
        LittleEndian::write_u32(&mut buf4, FREE_VER);
        f.write_all(&buf4)?; // ver
        LittleEndian::write_u32(&mut buf4, 0);
        f.write_all(&buf4)?; // reserved
        let _ = f.sync_all();

        Ok(Self { path })
    }

    /// Открыть существующий free‑лист и проверить заголовок.
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
        let mut buf4 = [0u8; 4];
        f.read_exact(&mut buf4)?; // ver
        let ver = LittleEndian::read_u32(&buf4);
        if ver != FREE_VER {
            return Err(anyhow!(
                "unsupported FREE version {} in {}",
                ver,
                path.display()
            ));
        }
        // reserved
        f.read_exact(&mut buf4)?;

        Ok(Self { path })
    }

    /// Текущее число свободных страниц.
    pub fn count(&self) -> Result<u64> {
        let len = std::fs::metadata(&self.path)?.len();
        if len < FREE_HDR_SIZE {
            return Err(anyhow!(
                "free file too small (< header): {}",
                self.path.display()
            ));
        }
        Ok((len - FREE_HDR_SIZE) / 8)
    }

    /// Добавить page_id в хвост списка.
    pub fn push(&self, page_id: u64) -> Result<()> {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)
            .with_context(|| format!("open free for push {}", self.path.display()))?;
        f.seek(SeekFrom::End(0))?;
        let mut buf8 = [0u8; 8];
        LittleEndian::write_u64(&mut buf8, page_id);
        f.write_all(&buf8)?;
        let _ = f.sync_all();
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
        if len < FREE_HDR_SIZE {
            return Err(anyhow!(
                "free file too small (< header): {}",
                self.path.display()
            ));
        }
        if len == FREE_HDR_SIZE {
            return Ok(None);
        }
        let last_off = len - 8;
        f.seek(SeekFrom::Start(last_off))?;
        let mut buf8 = [0u8; 8];
        f.read_exact(&mut buf8)?;
        let page_id = LittleEndian::read_u64(&buf8);

        f.set_len(last_off)?;
        let _ = f.sync_all();
        Ok(Some(page_id))
    }

    /// Путь к free‑файлу (для диагностики).
    pub fn path(&self) -> &Path {
        &self.path
    }
}
