// src/meta.rs — QuiverDB 2.0 (Meta v4)
//
// Формат <root>/meta (LE):
// MAGIC8 = "P2DBMETA"
// u32 version         = 4
// u32 page_size       (4 KiB..=1 MiB, power of two)
// u32 format_flags    (в этом файле сохраняется/читается в поле `flags` для совместимости API)
// u64 next_page_id
// u32 hash_kind       (1 = xxhash64(seed=0))
// u64 last_lsn
// u8  clean_shutdown  (1=clean, 0=unclean)
// u16 codec_default   (0=none,1=zstd,2=lz4)
// u8  checksum_kind   (0=crc32,1=crc32c,2=blake3_128)
//
// Политика:
// - Атомарная запись: tmp+rename, затем fsync родительского каталога (best‑effort на Windows).
// - validate_page_size: 4096..=1MiB, степень двойки.
// - checkpoint/commit‑логика будет опираться на last_lsn/clean_shutdown.

use anyhow::{anyhow, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// ---- Константы meta v4 ----

const META_MAGIC: &[u8; 8] = b"P2DBMETA";
const META_FILE: &str = "meta";

pub const HASH_KIND_XX64_SEED0: u32 = 1;

pub const CODEC_NONE: u16 = 0;
pub const CODEC_ZSTD: u16 = 1;
pub const CODEC_LZ4: u16 = 2;

pub const CKSUM_CRC32: u8 = 0;
pub const CKSUM_CRC32C: u8 = 1; // default policy
pub const CKSUM_BLAKE3_128: u8 = 2;

// ---- Структура заголовка (сохраняем имя MetaHeader для совместимости) ----

#[derive(Debug, Clone)]
pub struct MetaHeader {
    pub version: u32,      // == 4
    pub page_size: u32,    // 4 KiB .. 1 MiB (power of two)
    pub flags: u32,        // format_flags (v4) — сохраняем имя `flags` для совместимости
    pub next_page_id: u64,
    pub hash_kind: u32,    // 1 = xxhash64(seed=0)
    pub last_lsn: u64,
    pub clean_shutdown: bool,
    // Новые поля v4:
    pub codec_default: u16, // 0=none,1=zstd,2=lz4
    pub checksum_kind: u8,  // 0=crc32,1=crc32c,2=blake3_128
}

impl Default for MetaHeader {
    fn default() -> Self {
        Self {
            version: 4,
            page_size: 4096,
            flags: 0,
            next_page_id: 0,
            hash_kind: HASH_KIND_XX64_SEED0,
            last_lsn: 0,
            clean_shutdown: true,
            codec_default: CODEC_NONE,
            checksum_kind: CKSUM_CRC32C, // по умолчанию crc32c
        }
    }
}

// ---- Внутренние утилиты ----

#[inline]
fn meta_path(root: &Path) -> PathBuf {
    root.join(META_FILE)
}

#[cfg(unix)]
fn fsync_dir(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
    }
    Ok(())
}
#[cfg(not(unix))]
fn fsync_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Проверка корректности размера страницы (2^n, 4 KiB .. 1 MiB).
pub fn validate_page_size(page_size: u32) -> Result<()> {
    const MAX: u32 = 1 << 20; // 1 MiB
    if page_size < 4096 || page_size > MAX || (page_size & (page_size - 1)) != 0 {
        return Err(anyhow!(
            "page_size must be a power of two in [4096 .. 1048576], got {}",
            page_size
        ));
    }
    Ok(())
}

// ---- Запись/чтение meta v4 ----

/// Создать новый meta (v4). Ошибка, если уже существует.
pub fn write_meta_new(root: &Path, h: &MetaHeader) -> Result<()> {
    validate_page_size(h.page_size)?;

    let path = meta_path(root);
    if path.exists() {
        return Err(anyhow!("meta already exists at {}", path.display()));
    }

    let tmp = root.join(format!("{}.tmp", META_FILE));
    let _ = fs::remove_file(&tmp); // best‑effort

    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)
        .with_context(|| format!("open meta tmp {}", tmp.display()))?;

    write_meta_contents(&mut f, h)?;
    f.sync_all()?; // flush tmp to disk

    fs::rename(&tmp, &path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;

    let _ = fsync_dir(&path);
    Ok(())
}

/// Перезаписать meta (v4) через tmp+rename.
pub fn write_meta_overwrite(root: &Path, h: &MetaHeader) -> Result<()> {
    validate_page_size(h.page_size)?;

    let path = meta_path(root);
    let tmp = root.join(format!("{}.tmp", META_FILE));
    let _ = fs::remove_file(&tmp);

    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)
        .with_context(|| format!("open meta tmp {}", tmp.display()))?;

    write_meta_contents(&mut f, h)?;
    f.sync_all()?; // ensure tmp is on disk

    fs::rename(&tmp, &path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    let _ = fsync_dir(&path);
    Ok(())
}

/// Внутренняя запись полей meta v4 (offset=0).
fn write_meta_contents(f: &mut std::fs::File, h: &MetaHeader) -> Result<()> {
    f.seek(SeekFrom::Start(0))?;
    f.write_all(META_MAGIC)?;
    f.write_u32::<LittleEndian>(h.version)?;
    f.write_u32::<LittleEndian>(h.page_size)?;
    f.write_u32::<LittleEndian>(h.flags)?; // format_flags
    f.write_u64::<LittleEndian>(h.next_page_id)?;
    f.write_u32::<LittleEndian>(h.hash_kind)?;
    f.write_u64::<LittleEndian>(h.last_lsn)?;
    f.write_u8(if h.clean_shutdown { 1 } else { 0 })?;
    f.write_u16::<LittleEndian>(h.codec_default)?;
    f.write_u8(h.checksum_kind)?;
    Ok(())
}

/// Прочитать meta v4.
pub fn read_meta(root: &Path) -> Result<MetaHeader> {
    let path = meta_path(root);
    let mut f = OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open meta {}", path.display()))?;

    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    if &magic != META_MAGIC {
        return Err(anyhow!(
            "bad meta magic at {} (expected {:?}, got {:?})",
            path.display(),
            META_MAGIC,
            magic
        ));
    }

    let version = f.read_u32::<LittleEndian>()?;
    if version != 4 {
        return Err(anyhow!(
            "unsupported meta version {} at {} (expected 4)",
            version,
            path.display()
        ));
    }

    let page_size = f.read_u32::<LittleEndian>()?;
    let flags = f.read_u32::<LittleEndian>()?;
    let next_page_id = f.read_u64::<LittleEndian>()?;
    let hash_kind = f.read_u32::<LittleEndian>()?;
    let last_lsn = f.read_u64::<LittleEndian>()?;
    let clean_shutdown = f.read_u8()? != 0;
    let codec_default = f.read_u16::<LittleEndian>()?;
    let checksum_kind = f.read_u8()?;

    Ok(MetaHeader {
        version,
        page_size,
        flags,
        next_page_id,
        hash_kind,
        last_lsn,
        clean_shutdown,
        codec_default,
        checksum_kind,
    })
}

/// Пометить meta.clean_shutdown (best‑effort: только при изменении).
pub fn set_clean_shutdown(root: &Path, clean: bool) -> Result<()> {
    let mut m = read_meta(root)?;
    if m.clean_shutdown != clean {
        m.clean_shutdown = clean;
        write_meta_overwrite(root, &m)?;
    }
    Ok(())
}

/// Обновить last_lsn, если new_lsn больше текущего (best‑effort).
pub fn set_last_lsn(root: &Path, new_lsn: u64) -> Result<()> {
    let mut m = read_meta(root)?;
    if new_lsn > m.last_lsn {
        m.last_lsn = new_lsn;
        write_meta_overwrite(root, &m)?;
    }
    Ok(())
}

/// Утилита для инициализации meta v4 по параметрам.
pub fn init_meta_v4(
    root: &Path,
    page_size: u32,
    hash_kind: u32,
    codec_default: u16,
    checksum_kind: u8,
) -> Result<()> {
    let mut m = MetaHeader {
        page_size,
        hash_kind,
        codec_default,
        checksum_kind,
        ..MetaHeader::default()
    };
    m.version = 4;
    m.flags = 0;
    m.next_page_id = 0;
    m.last_lsn = 0;
    m.clean_shutdown = true;

    write_meta_new(root, &m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn meta_v4_roundtrip() {
        let root = std::env::temp_dir().join(format!("qdb2-meta-{}", nanos_for_test()));
        fs::create_dir_all(&root).unwrap();

        let m0 = MetaHeader {
            version: 4,
            page_size: 65536,
            flags: 0xAABBCCDD,
            next_page_id: 123,
            hash_kind: HASH_KIND_XX64_SEED0,
            last_lsn: 456,
            clean_shutdown: false,
            codec_default: CODEC_NONE,
            checksum_kind: CKSUM_CRC32C,
        };
        write_meta_new(&root, &m0).unwrap();

        let m1 = read_meta(&root).unwrap();
        assert_eq!(m1.version, 4);
        assert_eq!(m1.page_size, 65536);
        assert_eq!(m1.flags, 0xAABBCCDD);
        assert_eq!(m1.next_page_id, 123);
        assert_eq!(m1.hash_kind, HASH_KIND_XX64_SEED0);
        assert_eq!(m1.last_lsn, 456);
        assert!(!m1.clean_shutdown);

        set_clean_shutdown(&root, true).unwrap();
        let m2 = read_meta(&root).unwrap();
        assert!(m2.clean_shutdown);

        set_last_lsn(&root, 999).unwrap();
        let m3 = read_meta(&root).unwrap();
        assert_eq!(m3.last_lsn, 999);

        // overwrite and read again
        let mut m4 = m3.clone();
        m4.page_size = 131072;
        m4.codec_default = CODEC_ZSTD;
        write_meta_overwrite(&root, &m4).unwrap();
        let m5 = read_meta(&root).unwrap();
        assert_eq!(m5.page_size, 131072);
        assert_eq!(m5.codec_default, CODEC_ZSTD);
    }

    fn nanos_for_test() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }
}