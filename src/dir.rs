// src/dir.rs — QuiverDB 2.0 (Directory v2, sharded; single-shard implementation)
//
// Обновления:
// - compute_dir_crc считает CRC32C (Castagnoli), как в документации (P2DIR02).
// - NEW: быстрый путь set_heads_bulk_inplace (без tmp+rename), включается ENV P1_DIR_ATOMIC=0.
//   Опциональный fsync файла каталога ранее управлялся ENV P1_DIR_FSYNC.
// - FIX (safety): в inplace‑режиме мы теперь ВСЕГДА делаем fsync файла каталога после записи голов и CRC,
//   независимо от P1_DIR_FSYNC. Это снижает риск CRC mismatch при крэше в dev/bench режиме.
// - NEW: предупреждение один раз при включённом inplace-режиме (bench-only).
// - NEW: предупреждение один раз, если hash_kind != 1 (fallback на xxhash64(seed=0)).
//
// Формат:
// MAGIC8 = "P2DIR02\0"
// u32 version = 2
// u32 buckets
// u32 crc32c  (CRC32C over [version u32][buckets u32] + heads bytes)
// heads: buckets × u64 (LE), NO_PAGE = u64::MAX

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32c::crc32c;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub const DIR_MAGIC: &[u8; 8] = b"P2DIR02\0";
pub const DIR_VERSION: u32 = 2;

// Имя единственного шарда (пока)
const SHARD0_FILE: &str = "dir-000";

pub const NO_PAGE: u64 = u64::MAX;

// ---------------- CRC helper (CRC32C) ----------------

fn compute_dir_crc(version: u32, buckets: u32, heads_bytes: &[u8]) -> u32 {
    let mut buf = Vec::with_capacity(8 + heads_bytes.len());
    let mut tmp4 = [0u8; 4];
    LittleEndian::write_u32(&mut tmp4, version);
    buf.extend_from_slice(&tmp4);
    LittleEndian::write_u32(&mut tmp4, buckets);
    buf.extend_from_slice(&tmp4);
    buf.extend_from_slice(heads_bytes);
    crc32c(&buf)
}

#[cfg(unix)]
fn fsync_parent_dir(path: &Path) -> std::io::Result<()> {
    use std::fs::File;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
    }
    Ok(())
}
#[cfg(not(unix))]
fn fsync_parent_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

// --- ENV toggles (cached) ---

fn dir_use_atomic() -> bool {
    static ATOMIC: OnceLock<bool> = OnceLock::new();
    *ATOMIC.get_or_init(|| {
        std::env::var("P1_DIR_ATOMIC")
            .ok()
            .map(|s| s.trim().to_ascii_lowercase())
            .map(|s| !(s == "0" || s == "false" || s == "off" || s == "no"))
            .unwrap_or(true) // по умолчанию — атомарный путь
    })
}

// NEW: одноразовое предупреждение о неатомарном режиме
fn warn_inplace_once() {
    static WARNED: OnceLock<()> = OnceLock::new();
    WARNED.get_or_init(|| {
        eprintln!(
            "[WARN] Directory inplace mode enabled (P1_DIR_ATOMIC=0): \
             updates of dir-000 are not atomic; crash may leave CRC mismatch. \
             Use only for benchmarks/dev."
        );
    });
}

// NEW: одноразовое предупреждение про неподдержанный hash_kind
fn warn_hash_kind_once(kind: u32) {
    static WARNED: OnceLock<()> = OnceLock::new();
    WARNED.get_or_init(|| {
        eprintln!(
            "[WARN] Directory::bucket_of_key: hash_kind != 1 (got {}), \
             falling back to xxhash64(seed=0). Future versions may change hashing.",
            kind
        );
    });
}

#[derive(Debug)]
pub struct Directory {
    root: PathBuf,
    _shard_count: u32, // пока всегда 1
    pub bucket_count: u32,
}

impl Directory {
    #[inline]
    fn shard_path(&self, shard_no: u32) -> PathBuf {
        debug_assert_eq!(shard_no, 0);
        self.root.join(SHARD0_FILE)
    }

    fn shard_path_static(root: &Path, _shard_no: u32) -> PathBuf {
        root.join(SHARD0_FILE)
    }

    pub fn create(root: &Path, buckets: u32) -> Result<Self> {
        if buckets == 0 {
            return Err(anyhow!("buckets must be > 0"));
        }

        let path = Self::shard_path_static(root, 0);
        if path.exists() {
            return Err(anyhow!("directory shard already exists at {}", path.display()));
        }

        let mut f = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("create directory shard {}", path.display()))?;

        // header: magic(8) + version(4) + buckets(4) + crc(4 placeholder)
        f.write_all(DIR_MAGIC)?;
        let mut buf4 = [0u8; 4];

        LittleEndian::write_u32(&mut buf4, DIR_VERSION);
        f.write_all(&buf4)?;
        LittleEndian::write_u32(&mut buf4, buckets);
        f.write_all(&buf4)?;
        // crc placeholder
        LittleEndian::write_u32(&mut buf4, 0);
        f.write_all(&buf4)?;

        // heads (NO_PAGE)
        let mut heads_bytes = Vec::with_capacity(buckets as usize * 8);
        for _ in 0..buckets {
            let mut buf8 = [0u8; 8];
            LittleEndian::write_u64(&mut buf8, NO_PAGE);
            heads_bytes.extend_from_slice(&buf8);
        }
        f.write_all(&heads_bytes)?;

        // crc32c
        let crc = compute_dir_crc(DIR_VERSION, buckets, &heads_bytes);
        LittleEndian::write_u32(&mut buf4, crc);
        // seek и запись CRC в header
        f.seek(SeekFrom::Start(8 + 4 + 4))?;
        f.write_all(&buf4)?;

        let _ = f.sync_all();
        let _ = fsync_parent_dir(&path);

        Ok(Self {
            root: root.to_path_buf(),
            _shard_count: 1,
            bucket_count: buckets,
        })
    }

    pub fn open(root: &Path) -> Result<Self> {
        let path = Self::shard_path_static(root, 0);
        let mut f = OpenOptions::new()
            .read(true)
            .open(&path)
            .with_context(|| format!("open directory shard {}", path.display()))?;

        // magic
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        if &magic != DIR_MAGIC {
            return Err(anyhow!("bad directory magic at {}", path.display()));
        }

        // version
        let mut buf4 = [0u8; 4];
        f.read_exact(&mut buf4)?;
        let version = LittleEndian::read_u32(&buf4);
        if version != DIR_VERSION {
            return Err(anyhow!(
                "unsupported dir version {} at {}",
                version,
                path.display()
            ));
        }

        // buckets
        f.read_exact(&mut buf4)?;
        let buckets = LittleEndian::read_u32(&buf4);

        // stored crc
        f.read_exact(&mut buf4)?;
        let stored_crc = LittleEndian::read_u32(&buf4);

        // heads bytes
        let mut heads_bytes = vec![0u8; buckets as usize * 8];
        f.read_exact(&mut heads_bytes)?;

        if stored_crc != 0 {
            let calc = compute_dir_crc(version, buckets, &heads_bytes);
            if calc != stored_crc {
                return Err(anyhow!(
                    "directory CRC mismatch at {} (stored={}, calc={})",
                    path.display(),
                    stored_crc,
                    calc
                ));
            }
        }

        Ok(Self {
            root: root.to_path_buf(),
            _shard_count: 1,
            bucket_count: buckets,
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
        let path = self.shard_path(0);
        // смещение: magic(8) + version(4) + buckets(4) + crc(4) + bucket*8
        let offset = (8 + 4 + 4 + 4) as u64 + (bucket as u64) * 8;
        let mut f = OpenOptions::new().read(true).open(&path)?;
        f.seek(SeekFrom::Start(offset))?;
        let mut buf8 = [0u8; 8];
        f.read_exact(&mut buf8)?;
        Ok(LittleEndian::read_u64(&buf8))
    }

    /// writer-only: set_head(bucket, page_id)
    ///
    /// Атомарно обновляет shard (tmp+rename с CRC) либо быстро (in-place) —
    /// режим выбирается переменной окружения P1_DIR_ATOMIC (по умолчанию on).
    pub(crate) fn set_head(&self, bucket: u32, page_id: u64) -> Result<()> {
        self.set_heads_bulk(&[(bucket, page_id)])
    }

    /// writer-only: atomically update several heads at once.
    ///
    /// Правила:
    /// - updates может содержать несколько записей для одного bucket — берётся последняя.
    /// - Если updates пуст — NOP.
    /// - Валидация: все bucket < self.bucket_count.
    /// - Реализация:
    ///   * atomic (по умолчанию): старый путь tmp+rename и CRC.
    ///   * inplace (P1_DIR_ATOMIC=0): пишем heads прямо в файл, затем обновляем CRC в header.
    pub(crate) fn set_heads_bulk(&self, updates: &[(u32, u64)]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        // Валидация индексов
        for (b, _) in updates.iter() {
            if *b >= self.bucket_count {
                return Err(anyhow!(
                    "bucket {} out of range 0..{}",
                    b,
                    self.bucket_count - 1
                ));
            }
        }

        if dir_use_atomic() {
            self.set_heads_bulk_atomic(updates)
        } else {
            // bench-only предупреждение (один раз)
            warn_inplace_once();
            self.set_heads_bulk_inplace(updates)
        }
    }

    // ------- atomic path (tmp+rename) -------

    fn set_heads_bulk_atomic(&self, updates: &[(u32, u64)]) -> Result<()> {
        let path = self.shard_path(0);

        // Прочитаем header и heads в память
        let mut f = OpenOptions::new()
            .read(true)
            .open(&path)
            .with_context(|| format!("open directory shard {}", path.display()))?;

        // Header fields
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        if &magic != DIR_MAGIC {
            return Err(anyhow!("bad directory magic at {}", path.display()));
        }

        let mut buf4 = [0u8; 4];

        f.read_exact(&mut buf4)?; // version
        let version = LittleEndian::read_u32(&buf4);
        if version != DIR_VERSION {
            return Err(anyhow!("unsupported dir version {}", version));
        }

        f.read_exact(&mut buf4)?; // buckets
        let buckets = LittleEndian::read_u32(&buf4);
        if buckets != self.bucket_count {
            return Err(anyhow!(
                "buckets mismatch: header={}, struct={}",
                buckets,
                self.bucket_count
            ));
        }

        f.read_exact(&mut buf4)?; // stored crc (ignore)
        let _stored_crc = LittleEndian::read_u32(&buf4);

        let mut heads_bytes = vec![0u8; buckets as usize * 8];
        f.read_exact(&mut heads_bytes)?;

        // Применим обновления: последняя запись для bucket побеждает.
        let mut entries = updates.to_vec();
        entries.sort_by_key(|(b, _)| *b);

        let mut changed = false;
        let mut i = 0;
        while i < entries.len() {
            let b = entries[i].0;
            // Найдём последний индекс для этого же bucket
            let mut j = i + 1;
            while j < entries.len() && entries[j].0 == b {
                j += 1;
            }
            let (_, new_head) = entries[j - 1];
            // Применим
            let off = (b as usize) * 8;
            let cur = LittleEndian::read_u64(&heads_bytes[off..off + 8]);
            if cur != new_head {
                let mut buf8 = [0u8; 8];
                LittleEndian::write_u64(&mut buf8, new_head);
                heads_bytes[off..off + 8].copy_from_slice(&buf8);
                changed = true;
            }
            i = j;
        }

        if !changed {
            // Нечего писать
            return Ok(());
        }

        // Запишем tmp + rename
        let tmp = path.with_file_name(format!(
            "{}.tmp",
            path.file_name().unwrap().to_string_lossy()
        ));
        let _ = std::fs::remove_file(&tmp);

        let mut tf = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .with_context(|| format!("open tmp {}", tmp.display()))?;

        // header
        tf.write_all(DIR_MAGIC)?;
        LittleEndian::write_u32(&mut buf4, version);
        tf.write_all(&buf4)?;
        LittleEndian::write_u32(&mut buf4, buckets);
        tf.write_all(&buf4)?;
        // crc placeholder
        LittleEndian::write_u32(&mut buf4, 0);
        tf.write_all(&buf4)?;
        // heads
        tf.write_all(&heads_bytes)?;

        // CRC32C и запись в header
        let crc = compute_dir_crc(version, buckets, &heads_bytes);
        LittleEndian::write_u32(&mut buf4, crc);
        tf.seek(SeekFrom::Start(8 + 4 + 4))?;
        tf.write_all(&buf4)?;
        let _ = tf.sync_all();

        std::fs::rename(&tmp, &path)?;
        let _ = fsync_parent_dir(&path);
        Ok(())
    }

    // ------- inplace path (no rename) -------

    fn set_heads_bulk_inplace(&self, updates: &[(u32, u64)]) -> Result<()> {
        let path = self.shard_path(0);
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open directory shard {}", path.display()))?;

        // validate header and load current heads
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        if &magic != DIR_MAGIC {
            return Err(anyhow!("bad directory magic at {}", path.display()));
        }
        let mut buf4 = [0u8; 4];
        f.read_exact(&mut buf4)?; // version
        let version = LittleEndian::read_u32(&buf4);
        if version != DIR_VERSION {
            return Err(anyhow!("unsupported dir version {}", version));
        }
        f.read_exact(&mut buf4)?; // buckets
        let buckets = LittleEndian::read_u32(&buf4);
        if buckets != self.bucket_count {
            return Err(anyhow!(
                "buckets mismatch: header={}, struct={}",
                buckets,
                self.bucket_count
            ));
        }
        // read stored CRC (ignore value)
        f.read_exact(&mut buf4)?;
        let _stored_crc = LittleEndian::read_u32(&buf4);

        // read heads
        let mut heads_bytes = vec![0u8; buckets as usize * 8];
        f.read_exact(&mut heads_bytes)?;

        // apply updates (last for each bucket wins)
        let mut entries = updates.to_vec();
        entries.sort_by_key(|(b, _)| *b);

        let mut changed = false;
        let mut i = 0;
        while i < entries.len() {
            let b = entries[i].0;
            // last index for this bucket
            let mut j = i + 1;
            while j < entries.len() && entries[j].0 == b {
                j += 1;
            }
            let (_, new_head) = entries[j - 1];
            let off = (b as usize) * 8;
            let cur = LittleEndian::read_u64(&heads_bytes[off..off + 8]);
            if cur != new_head {
                let mut buf8 = [0u8; 8];
                LittleEndian::write_u64(&mut buf8, new_head);
                heads_bytes[off..off + 8].copy_from_slice(&buf8);
                changed = true;
            }
            i = j;
        }

        if !changed {
            return Ok(());
        }

        // write back heads bytes
        let heads_off = (8 + 4 + 4 + 4) as u64;
        f.seek(SeekFrom::Start(heads_off))?;
        f.write_all(&heads_bytes)?;

        // compute and update CRC in header
        let crc = compute_dir_crc(version, buckets, &heads_bytes);
        LittleEndian::write_u32(&mut buf4, crc);
        f.seek(SeekFrom::Start((8 + 4 + 4) as u64))?;
        f.write_all(&buf4)?;

        // FIX: в inplace‑режиме всегда fsync, независимо от ENV
        let _ = f.sync_all();

        Ok(())
    }

    /// Вычислить bucket для ключа (xxhash64(seed=0)).
    pub fn bucket_of_key(&self, key: &[u8], hash_kind: u32) -> u32 {
        if hash_kind != 1 {
            warn_hash_kind_once(hash_kind);
        }
        let h = hash64_xxseed0(key, hash_kind);
        (h % (self.bucket_count as u64)) as u32
    }

    /// Подсчитать количество используемых bucket'ов (head != NO_PAGE).
    pub fn count_used_buckets(&self) -> Result<u32> {
        let path = self.shard_path(0);
        let mut f = OpenOptions::new().read(true).open(&path)?;
        f.seek(SeekFrom::Start((8 + 4 + 4 + 4) as u64))?;
        let mut used = 0u32;
        let mut buf8 = [0u8; 8];
        for _ in 0..self.bucket_count {
            f.read_exact(&mut buf8)?;
            let pid = LittleEndian::read_u64(&buf8);
            if pid != NO_PAGE {
                used += 1;
            }
        }
        Ok(used)
    }
}

// xxhash64(seed=0) helper
fn hash64_xxseed0(key: &[u8], _hash_kind: u32) -> u64 {
    use std::hash::Hasher;
    let mut h = twox_hash::XxHash64::with_seed(0);
    h.write(key);
    h.finish()
}