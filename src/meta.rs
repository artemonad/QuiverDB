use crate::consts::{MAGIC, META_FILE};
use crate::hash::{HashKind, HASH_KIND_DEFAULT};
use anyhow::{anyhow, Context, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug, Clone)]
pub struct MetaHeader {
    pub version: u32,
    pub page_size: u32,
    pub flags: u32,
    pub next_page_id: u64,
    // v2+: стабильный вид хеширования ключей для bucket-распределения
    pub hash_kind: HashKind,
    // v3+: журналирование
    pub last_lsn: u64,
    pub clean_shutdown: bool,
}

pub fn validate_page_size(page_size: u32) -> Result<()> {
    if page_size < 4096 || (page_size & (page_size - 1)) != 0 {
        return Err(anyhow!(
            "page_size must be a power of two and >= 4096, got {}",
            page_size
        ));
    }
    if page_size > u16::MAX as u32 {
        return Err(anyhow!(
            "page_size must be <= {}, slotted pages use 16-bit offsets",
            u16::MAX
        ));
    }
    Ok(())
}

// Best-effort fsync родительской директории после rename.
// На Unix открываем директорию и делаем sync_all(); на Windows делаем no-op.
#[cfg(unix)]
fn fsync_dir(path: &Path) -> std::io::Result<()> {
    let dir = File::open(path)?;
    dir.sync_all()
}
#[cfg(not(unix))]
fn fsync_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

pub fn write_meta_new(root: &Path, h: &MetaHeader) -> Result<()> {
    let meta_path = root.join(META_FILE);
    if meta_path.exists() {
        return Err(anyhow!(
            "meta already exists at {}",
            meta_path.display()
        ));
    }
    let tmp_path = root.join(format!("{}.tmp", META_FILE));
    // best-effort: удалить старый tmp, если остался после сбоя
    let _ = fs::remove_file(&tmp_path);

    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .with_context(|| format!("open meta tmp {}", tmp_path.display()))?;
    write_meta_contents(&mut f, h)?;
    f.sync_all()?; // гарантируем, что содержимое tmp на диске

    // Атомарная замена: tmp -> meta
    fs::rename(&tmp_path, &meta_path).with_context(|| {
        format!(
            "rename {} -> {}",
            tmp_path.display(),
            meta_path.display()
        )
    })?;

    // Зафиксируем rename в каталоге
    let _ = fsync_dir(root);
    Ok(())
}

pub fn write_meta_overwrite(root: &Path, h: &MetaHeader) -> Result<()> {
    let meta_path = root.join(META_FILE);
    let tmp_path = root.join(format!("{}.tmp", META_FILE));
    // best-effort: удалить старый tmp, если остался после сбоя
    let _ = fs::remove_file(&tmp_path);

    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .with_context(|| format!("open meta tmp {}", tmp_path.display()))?;
    f.seek(SeekFrom::Start(0))?;
    write_meta_contents(&mut f, h)?;
    f.sync_all()?; // убедимся, что tmp записан на диск

    // Атомарная замена существующего meta
    fs::rename(&tmp_path, &meta_path).with_context(|| {
        format!(
            "rename {} -> {}",
            tmp_path.display(),
            meta_path.display()
        )
    })?;

    // Best-effort fsync каталога
    let _ = fsync_dir(root);
    Ok(())
}

fn write_meta_contents(f: &mut std::fs::File, h: &MetaHeader) -> Result<()> {
    // Формат v3:
    // [MAGIC8]
    // [version u32]
    // [page_size u32]
    // [flags u32]
    // [next_page_id u64]
    // [hash_kind u32]            (v2+)
    // [last_lsn u64]             (v3+)
    // [clean_shutdown u8]        (v3+)
    // (опционально сейчас: без паддинга)

    f.seek(SeekFrom::Start(0))?;
    f.write_all(MAGIC)?;
    f.write_u32::<LittleEndian>(h.version)?;
    f.write_u32::<LittleEndian>(h.page_size)?;
    f.write_u32::<LittleEndian>(h.flags)?;
    f.write_u64::<LittleEndian>(h.next_page_id)?;

    // v2+
    f.write_u32::<LittleEndian>(h.hash_kind.to_u32())?;

    // v3+
    f.write_u64::<LittleEndian>(h.last_lsn)?;
    f.write_u8(if h.clean_shutdown { 1 } else { 0 })?;

    Ok(())
}

pub fn read_meta(root: &Path) -> Result<MetaHeader> {
    let meta_path = root.join(META_FILE);
    let mut f = OpenOptions::new()
        .read(true)
        .open(&meta_path)
        .with_context(|| format!("open meta for read {}", meta_path.display()))?;

    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(anyhow!(
            "bad magic in meta: expected {:?}, got {:?}",
            MAGIC,
            magic
        ));
    }

    let version = f.read_u32::<LittleEndian>()?;
    let page_size = f.read_u32::<LittleEndian>()?;
    let flags = f.read_u32::<LittleEndian>()?;
    let next_page_id = f.read_u64::<LittleEndian>()?;

    // v1: поля заканчивались на next_page_id.
    // v2+: добавлено поле hash_kind u32.
    let hash_kind = if version >= 2 {
        let code = f
            .read_u32::<LittleEndian>()
            .with_context(|| "meta v2: missing hash_kind field")?;
        HashKind::from_u32(code)
            .ok_or_else(|| anyhow!("unknown hash_kind code {} in meta", code))?
    } else {
        // Совместимость: если meta v1 — используем дефолтный стабильный хеш.
        HASH_KIND_DEFAULT
    };

    // v3+: last_lsn u64 и clean_shutdown u8
    let (last_lsn, clean_shutdown) = if version >= 3 {
        let lsn = f
            .read_u64::<LittleEndian>()
            .with_context(|| "meta v3: missing last_lsn")?;
        let clean = f
            .read_u8()
            .with_context(|| "meta v3: missing clean_shutdown")?;
        (lsn, clean != 0)
    } else {
        (0u64, true) // по умолчанию считаем чистое завершение, если старый формат
    };

    Ok(MetaHeader {
        version,
        page_size,
        flags,
        next_page_id,
        hash_kind,
        last_lsn,
        clean_shutdown,
    })
}

/// Установить флаг «чистого завершения».
pub fn set_clean_shutdown(root: &Path, clean: bool) -> Result<()> {
    let mut m = read_meta(root)?;
    if m.clean_shutdown != clean {
        m.clean_shutdown = clean;
        write_meta_overwrite(root, &m)?;
    }
    Ok(())
}

/// Обновить last_lsn (если больше текущего).
pub fn set_last_lsn(root: &Path, new_lsn: u64) -> Result<()> {
    let mut m = read_meta(root)?;
    if new_lsn > m.last_lsn {
        m.last_lsn = new_lsn;
        write_meta_overwrite(root, &m)?;
    }
    Ok(())
}