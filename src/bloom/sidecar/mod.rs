//! bloom/sidecar — реализация BloomSidecar (P2BLM01) + интеграция с глобальным кэшем.
//!
//! Разнесение:
//! - open.rs — логика открытия/создания sidecar (open_ro/open/create/open_or_create_for_db).
//! - ops.rs  — операции (rebuild_all/rebuild_bucket/test/update/set_last_lsn/is_fresh_for_db).
//! - mod.rs  — общие типы, константы формата, ENV тумблеры, lock, view и низкоуровневые helpers.
//!
//! Формат (LE) v2 (P2BLM01):
//!   [magic8="P2BLM01\0"]
//!   [version u32=2]
//!   [buckets u32]
//!   [bytes_per_bucket u32]
//!   [k_hashes u32]
//!   [seed1 u64]
//!   [seed2 u64]
//!   [last_lsn u64]
//! Body: buckets × bytes_per_bucket (битовые фильтры подряд).
//!
//! last_lsn в заголовке должен совпадать с meta.last_lsn БД (fresh state).
//! Любые изменения — под файловым lock и с sync_all().

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use fs2::FileExt;
use memmap2::{Mmap, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// Дадим доступ подмодулям к cache API через re-export из верхнего модуля bloom::cache.
pub use super::cache::{bloom_cache_get, bloom_cache_put};

// -------------------- константы формата (видимы в подмодулях) --------------------

pub(super) const MAGIC: &[u8; 8] = b"P2BLM01\0";
pub(super) const VERSION_V1: u32 = 1;
pub(super) const VERSION_V2: u32 = 2;

// Размеры заголовков
pub(super) const HDR_SIZE_V1_USIZE: usize = 40;
pub(super) const HDR_SIZE_V1_U64: u64 = 40;
pub(super) const HDR_SIZE_V2_USIZE: usize = 48;
pub(super) const HDR_SIZE_V2_U64: u64 = 48;

// Смещения полей v2 (первые поля совпадают с v1)
pub(super) const OFF_MAGIC: usize = 0;
pub(super) const OFF_VERSION: usize = 8;
pub(super) const OFF_BUCKETS: usize = 12;
pub(super) const OFF_BYTES_PER_BUCKET: usize = 16;
pub(super) const OFF_K_HASHES: usize = 20;
pub(super) const OFF_SEED1: usize = 24;
pub(super) const OFF_SEED2: usize = 32;
pub(super) const OFF_LAST_LSN: usize = 40;

// -------------------- ENV toggles (видимы подмодулям) --------------------

pub(super) fn bloom_mmap_enabled() -> bool {
    std::env::var("P1_BLOOM_MMAP")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

fn bloom_lock_path(root: &Path) -> PathBuf {
    root.join("bloom.bin.lock")
}

pub(super) fn lock_bloom_file(root: &Path) -> Result<File> {
    let lp = bloom_lock_path(root);
    let f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lp)
        .with_context(|| format!("open lock {}", lp.display()))?;
    f.lock_exclusive()
        .with_context(|| format!("lock_exclusive {}", lp.display()))?;
    Ok(f)
}

// -------------------- структура метаданных и основной тип --------------------

#[derive(Debug, Clone)]
pub struct BloomMeta {
    pub buckets: u32,
    pub bytes_per_bucket: u32,
    pub k_hashes: u32,
    pub seed1: u64,
    pub seed2: u64,
    pub last_lsn: u64, // v2: LSN консистентности; 0 для v1
}

#[derive(Debug)]
pub struct BloomSidecar {
    pub(crate) root: PathBuf,
    pub(crate) path: PathBuf,
    pub(crate) meta: BloomMeta,
    pub(crate) hdr_size_u64: u64,   // 40 (v1) или 48 (v2)
    pub(crate) hdr_size_usize: usize,
    pub(crate) version: u32,

    // Постоянный RO‑хэндл (для mmap и fallback чтений)
    pub(crate) f_ro: Option<Mutex<File>>,

    // RAM‑копия body (если выбрали RAM режим)
    pub(crate) body: Option<Box<[u8]>>,

    // MMAP view на body (если выбрали mmap режим)
    pub(crate) mmap: Option<Mmap>,
}

// -------------------- общие helpers и view‑логика (для подмодулей) --------------------

impl BloomSidecar {
    // Перечитать views (mmap/RAM) — общая логика для open/ops
    pub(crate) fn reload_views(&mut self) -> Result<()> {
        let total_body_len = (self.meta.buckets as usize)
            .saturating_mul(self.meta.bytes_per_bucket as usize);

        if bloom_mmap_enabled() {
            if let Some(ref ro) = self.f_ro {
                let file = ro.lock().map_err(|_| anyhow!("bloom ro handle poisoned"))?;
                if total_body_len == 0 {
                    self.mmap = None;
                } else {
                    let mmap = unsafe {
                        MmapOptions::new()
                            .offset(self.hdr_size_u64)
                            .len(total_body_len)
                            .map(&*file)
                            .map_err(|e| anyhow!("bloom mmap reload: {}", e))?
                    };
                    self.mmap = Some(mmap);
                }
                self.body = None;
            } else {
                self.body = Some(read_body_from_path(&self.path, self.hdr_size_u64, total_body_len)?);
                self.mmap = None;
            }
        } else {
            self.body = Some(read_body_from_path(&self.path, self.hdr_size_u64, total_body_len)?);
            self.mmap = None;
        }
        Ok(())
    }

    // Записать в заголовок last_lsn (v2) под открытым файловым хэндлом — общая логика
    pub(crate) fn write_header_last_lsn_locked(&mut self, f: &mut File, last_lsn: u64) -> Result<()> {
        if self.version != VERSION_V2 {
            return Ok(());
        }
        let mut hdr = vec![0u8; self.hdr_size_usize];
        f.seek(SeekFrom::Start(0))?;
        f.read_exact(&mut hdr)?;
        LittleEndian::write_u64(&mut hdr[OFF_LAST_LSN..OFF_LAST_LSN + 8], last_lsn);
        f.seek(SeekFrom::Start(0))?;
        f.write_all(&hdr)?;
        Ok(())
    }

    // Поставить биты ключа в bitmap (общая логика)
    pub(crate) fn add_key_to_bits(&self, key: &[u8], bits: &mut [u8]) {
        let bpb = self.meta.bytes_per_bucket as usize;
        let nbits = (bpb * 8) as u64;

        let h1 = xxh64(key, self.meta.seed1);
        let h2 = xxh64(key, self.meta.seed2);

        for i in 0..self.meta.k_hashes {
            let hv = h1.wrapping_add((i as u64).wrapping_mul(h2));
            let bit = (hv % nbits) as usize;
            set_bit(bits, bit);
        }
    }

    // Проверить биты ключа в bitmap (общая логика)
    pub(crate) fn test_in_bits(&self, key: &[u8], bits: &[u8]) -> bool {
        let bpb = self.meta.bytes_per_bucket as usize;
        let nbits = (bpb * 8) as u64;

        let h1 = xxh64(key, self.meta.seed1);
        let h2 = xxh64(key, self.meta.seed2);

        for i in 0..self.meta.k_hashes {
            let hv = h1.wrapping_add((i as u64).wrapping_mul(h2));
            let bit = (hv % nbits) as usize;
            if !get_bit(bits, bit) {
                return false;
            }
        }
        true
    }

    // -------- Публичные геттеры (используются внешним кодом) --------

    /// Количество бакетов.
    #[inline]
    pub fn buckets(&self) -> u32 {
        self.meta.buckets
    }

    /// Байт на бакет.
    #[inline]
    pub fn bytes_per_bucket(&self) -> u32 {
        self.meta.bytes_per_bucket
    }

    /// Число хэшей.
    #[inline]
    pub fn k_hashes(&self) -> u32 {
        self.meta.k_hashes
    }

    /// last_lsn из заголовка bloom (v2; 0 для v1).
    #[inline]
    pub fn last_lsn(&self) -> u64 {
        self.meta.last_lsn
    }
}

// Низкоуровневое чтение body в RAM (используется open/reload)
pub(super) fn read_body_from_path(path: &Path, offset: u64, len: usize) -> Result<Box<[u8]>> {
    let mut f = OpenOptions::new()
        .read(true)
        .open(path)
        .with_context(|| format!("open {} for RAM-read", path.display()))?;
    let mut buf = vec![0u8; len];
    if len > 0 {
        f.seek(SeekFrom::Start(offset))?;
        f.read_exact(&mut buf)?;
    }
    Ok(buf.into_boxed_slice())
}

// Битовые операции/хеш — внутренние helpers

#[inline]
fn set_bit(bytes: &mut [u8], bit: usize) {
    let byte = bit / 8;
    let mask = 1u8 << (bit % 8);
    bytes[byte] |= mask;
}

#[inline]
fn get_bit(bytes: &[u8], bit: usize) -> bool {
    let byte = bit / 8;
    let mask = 1u8 << (bit % 8);
    (bytes[byte] & mask) != 0
}

#[inline]
fn xxh64(data: &[u8], seed: u64) -> u64 {
    use std::hash::Hasher;
    let mut h = twox_hash::XxHash64::with_seed(seed);
    h.write(data);
    h.finish()
}

// -------------------- подключение подмодулей --------------------

pub mod open;
pub mod ops;