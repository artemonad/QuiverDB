// src/dir.rs

use crate::consts::{DIR_FILE, DIR_HDR_SIZE, DIR_MAGIC, NO_PAGE};
use crate::hash::{bucket_of_key, HashKind};
use crate::meta::read_meta;
use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher as Crc32;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct Directory {
    pub path: PathBuf,
    pub bucket_count: u32,
    pub hash_kind: HashKind,
}

impl Directory {
    pub fn create(root: &Path, buckets: u32) -> Result<Self> {
        if buckets == 0 {
            return Err(anyhow!("buckets must be > 0"));
        }
        // meta уже должна существовать — берём из неё выбранный стабильный хеш
        let meta = read_meta(root).context("read meta for directory create")?;
        let path = root.join(DIR_FILE);
        let mut f = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)
            .with_context(|| format!("create dir {}", path.display()))?;

        // header (magic + version + buckets + reserved=0)
        f.write_all(DIR_MAGIC)?;
        f.write_u32::<LittleEndian>(1)?; // version
        f.write_u32::<LittleEndian>(buckets)?;
        f.write_u64::<LittleEndian>(0)?; // reserved (позже запишем CRC)

        // heads: NO_PAGE
        let mut heads_bytes = Vec::with_capacity(buckets as usize * 8);
        for _ in 0..buckets {
            let mut buf8 = [0u8; 8];
            LittleEndian::write_u64(&mut buf8, NO_PAGE);
            heads_bytes.extend_from_slice(&buf8);
        }
        f.write_all(&heads_bytes)?;

        // посчитаем CRC (по [version u32][buckets u32] + heads_bytes)
        let crc = compute_dir_crc(1, buckets, &heads_bytes);
        // запишем CRC в поле reserved (u64, низ 32 бита)
        f.seek(SeekFrom::Start(8 + 4 + 4))?; // magic8 + ver4 + buckets4
        f.write_u64::<LittleEndian>(crc as u64)?;

        f.sync_all()?;
        Ok(Self {
            path,
            bucket_count: buckets,
            hash_kind: meta.hash_kind,
        })
    }

    pub fn open(root: &Path) -> Result<Self> {
        let meta = read_meta(root).context("read meta for directory open")?;
        let path = root.join(DIR_FILE);
        let mut f = OpenOptions::new()
            .read(true)
            .open(&path)
            .with_context(|| format!("open dir {}", path.display()))?;

        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        if &magic != DIR_MAGIC {
            return Err(anyhow!("bad DIR magic in {}", path.display()));
        }
        let version = f.read_u32::<LittleEndian>()?;
        if version != 1 {
            return Err(anyhow!(
                "unsupported DIR version {} in {}",
                version,
                path.display()
            ));
        }
        let buckets = f.read_u32::<LittleEndian>()?;
        let reserved_crc = f.read_u64::<LittleEndian>()?;

        // Прочитаем heads как bytes для CRC проверки
        let heads_len = (buckets as usize) * 8;
        let mut heads_bytes = vec![0u8; heads_len];
        f.read_exact(&mut heads_bytes)?;

        // Проверка CRC, если в файле он установлен (reserved_crc != 0).
        if reserved_crc != 0 {
            let calc = compute_dir_crc(version, buckets, &heads_bytes);
            if calc as u64 != reserved_crc {
                return Err(anyhow!(
                    "DIR CRC mismatch in {} (stored={}, calc={})",
                    path.display(),
                    reserved_crc,
                    calc
                ));
            }
        }

        Ok(Self {
            path,
            bucket_count: buckets,
            hash_kind: meta.hash_kind,
        })
    }

    pub fn head(&self, bucket: u32) -> Result<u64> {
        if bucket >= self.bucket_count {
            return Err(anyhow!(
                "bucket {} out of range 0..{}",
                bucket,
                self.bucket_count - 1
            ));
        }
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        let off = DIR_HDR_SIZE as u64 + (bucket as u64) * 8;
        f.seek(SeekFrom::Start(off))?;
        let mut buf = [0u8; 8];
        f.read_exact(&mut buf)?;
        Ok(LittleEndian::read_u64(&buf))
    }

    /// set_head теперь делает атомарное обновление: tmp+rename (+ fsync каталога best-effort).
    pub fn set_head(&self, bucket: u32, page_id: u64) -> Result<()> {
        if bucket >= self.bucket_count {
            return Err(anyhow!(
                "bucket {} out of range 0..{}",
                bucket,
                self.bucket_count - 1
            ));
        }

        // 1) Прочитать текущие heads в память
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        if &magic != DIR_MAGIC {
            return Err(anyhow!("bad DIR magic in {}", self.path.display()));
        }
        let version = f.read_u32::<LittleEndian>()?;
        if version != 1 {
            return Err(anyhow!(
                "unsupported DIR version {} in {}",
                version,
                self.path.display()
            ));
        }
        let buckets = f.read_u32::<LittleEndian>()?;
        let _reserved_crc = f.read_u64::<LittleEndian>()?;

        if buckets != self.bucket_count {
            return Err(anyhow!(
                "bucket_count mismatch: header={}, struct={}",
                buckets,
                self.bucket_count
            ));
        }

        let heads_len = (buckets as usize) * 8;
        let mut heads_bytes = vec![0u8; heads_len];
        f.read_exact(&mut heads_bytes)?;

        // Обновить нужный bucket
        let off = (bucket as usize) * 8;
        LittleEndian::write_u64(&mut heads_bytes[off..off + 8], page_id);

        // 2) Создать tmp-файл, записать header+heads, посчитать CRC и вписать.
        let tmp_path = self
            .path
            .with_file_name(format!("{}.tmp", self.path.file_name().unwrap().to_string_lossy()));
        // best-effort удалить старый tmp
        let _ = std::fs::remove_file(&tmp_path);

        let mut tf = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .with_context(|| format!("open dir tmp {}", tmp_path.display()))?;

        // header
        tf.write_all(DIR_MAGIC)?;
        tf.write_u32::<LittleEndian>(version)?;
        tf.write_u32::<LittleEndian>(buckets)?;
        tf.write_u64::<LittleEndian>(0)?; // reserved (CRC позже)
        // heads
        tf.write_all(&heads_bytes)?;

        // CRC и запись в reserved
        let crc = compute_dir_crc(version, buckets, &heads_bytes);
        tf.seek(SeekFrom::Start(8 + 4 + 4))?;
        tf.write_u64::<LittleEndian>(crc as u64)?;
        tf.sync_all()?;

        // 3) Атомарная замена: tmp -> dir
        std::fs::rename(&tmp_path, &self.path).with_context(|| {
            format!(
                "rename dir tmp {} -> {}",
                tmp_path.display(),
                self.path.display()
            )
        })?;

        // 4) Зафиксировать rename в каталоге (Unix). На Windows — no-op.
        let _ = fsync_parent_dir(&self.path);

        Ok(())
    }

    pub fn bucket_of_key(&self, key: &[u8]) -> u32 {
        bucket_of_key(self.hash_kind, key, self.bucket_count)
    }

    pub fn count_used_buckets(&self) -> Result<u32> {
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        f.seek(SeekFrom::Start(DIR_HDR_SIZE as u64))?;
        let mut used = 0u32;
        for _ in 0..self.bucket_count {
            let pid = f.read_u64::<LittleEndian>()?;
            if pid != NO_PAGE {
                used += 1;
            }
        }
        Ok(used)
    }
}

/// CRC по [version u32][buckets u32] + heads_bytes.
/// Magic и поле reserved не входят в расчёт (reserved содержит CRC32).
fn compute_dir_crc(version: u32, buckets: u32, heads_bytes: &[u8]) -> u32 {
    let mut hasher = Crc32::new();
    let mut buf4 = [0u8; 4];
    LittleEndian::write_u32(&mut buf4, version);
    hasher.update(&buf4);
    LittleEndian::write_u32(&mut buf4, buckets);
    hasher.update(&buf4);
    hasher.update(heads_bytes);
    hasher.finalize()
}

// Best-effort fsync parent directory after rename (Unix only).
#[cfg(unix)]
fn fsync_parent_dir(p: &Path) -> std::io::Result<()> {
    use std::fs::File;
    if let Some(parent) = p.parent() {
        if !parent.as_os_str().is_empty() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
    }
    Ok(())
}
#[cfg(not(unix))]
fn fsync_parent_dir(_p: &Path) -> std::io::Result<()> {
    Ok(())
}