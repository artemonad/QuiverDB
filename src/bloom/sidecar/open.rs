//! bloom/sidecar/open — открытие/создание BloomSidecar (P2BLM01).
//!
//! Шаг разнесения: переносим сюда логику open_ro/open/create/open_or_create_for_db.
//! В этом шаге меняем стратегию MMAP: вместо offset=hdr_size (не выровнено по странице)
//! мапим ВЕСЬ файл с offset=0 и длиной = file.len(). Индексация тела учитывает hdr_size.
//!
//! Примечание:
//! - Поддержка “старого” режима (mmap только body) уже добавлена в ops::test(),
//!   поэтому переход безопасен: test() распознаёт оба случая и корректно вычисляет базу.

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use memmap2::{Mmap, MmapOptions};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use crate::db::core::Db;

use super::{
    BloomMeta, BloomSidecar,
    MAGIC, VERSION_V1, VERSION_V2,
    HDR_SIZE_V1_USIZE, HDR_SIZE_V1_U64,
    HDR_SIZE_V2_USIZE, HDR_SIZE_V2_U64,
    OFF_MAGIC, OFF_VERSION, OFF_BUCKETS, OFF_BYTES_PER_BUCKET, OFF_K_HASHES,
    OFF_SEED1, OFF_SEED2, OFF_LAST_LSN,
    bloom_mmap_enabled, read_body_from_path,
};

impl BloomSidecar {
    /// Открыть существующий bloom.bin (RO).
    pub fn open_ro(root: &Path) -> Result<Self> {
        Self::open_inner(root, false)
    }

    /// Открыть существующий bloom.bin (RW).
    pub fn open(root: &Path) -> Result<Self> {
        Self::open_inner(root, true)
    }

    /// Внутренний путь открытия: читаем заголовок, настраиваем RAM/MMAP view.
    pub(crate) fn open_inner(root: &Path, write: bool) -> Result<Self> {
        let path = root.join("bloom.bin");
        let mut opts = OpenOptions::new();
        opts.read(true);
        if write {
            opts.write(true);
        }
        let mut f = opts
            .open(&path)
            .with_context(|| format!("open bloom {}", path.display()))?;

        // Header пролог (magic+version)
        let mut prefix = [0u8; 12];
        f.seek(SeekFrom::Start(0))?;
        f.read_exact(&mut prefix)?;
        if &prefix[OFF_MAGIC..OFF_MAGIC + 8] != MAGIC {
            return Err(anyhow!("bad bloom magic at {}", path.display()));
        }
        let version = LittleEndian::read_u32(&prefix[OFF_VERSION..OFF_VERSION + 4]);
        let (hdr_size_usize, hdr_size_u64) = match version {
            VERSION_V1 => (HDR_SIZE_V1_USIZE, HDR_SIZE_V1_U64),
            VERSION_V2 => (HDR_SIZE_V2_USIZE, HDR_SIZE_V2_U64),
            other => {
                return Err(anyhow!(
                    "unsupported bloom version {} at {}",
                    other,
                    path.display()
                ));
            }
        };

        // Полный заголовок
        let mut hdr = vec![0u8; hdr_size_usize];
        f.seek(SeekFrom::Start(0))?;
        f.read_exact(&mut hdr)?;

        let buckets = LittleEndian::read_u32(&hdr[OFF_BUCKETS..OFF_BUCKETS + 4]);
        let bytes_per_bucket = LittleEndian::read_u32(&hdr[OFF_BYTES_PER_BUCKET..OFF_BYTES_PER_BUCKET + 4]);
        let k_hashes = LittleEndian::read_u32(&hdr[OFF_K_HASHES..OFF_K_HASHES + 4]);
        let seed1 = LittleEndian::read_u64(&hdr[OFF_SEED1..OFF_SEED1 + 8]);
        let seed2 = LittleEndian::read_u64(&hdr[OFF_SEED2..OFF_SEED2 + 8]);
        let last_lsn = if version == VERSION_V2 {
            LittleEndian::read_u64(&hdr[OFF_LAST_LSN..OFF_LAST_LSN + 8])
        } else {
            0
        };

        // RO-хэндл для mmap/fallback чтений
        let f_ro = match OpenOptions::new().read(true).open(&path) {
            Ok(file) => Some(Mutex::new(file)),
            Err(_) => None,
        };

        // Выбор RAM/MMAP
        let total_body_len = (buckets as usize).saturating_mul(bytes_per_bucket as usize);

        let mut body_opt: Option<Box<[u8]>> = None;
        let mut mmap_opt: Option<Mmap> = None;

        if bloom_mmap_enabled() && total_body_len > 0 {
            if let Some(ro) = &f_ro {
                let file = ro.lock().map_err(|_| anyhow!("bloom ro handle poisoned"))?;
                // Новый безопасный режим: мапим ВЕСЬ файл от offset=0 (page-aligned)
                let flen = file.metadata()?.len() as usize;
                if flen >= hdr_size_usize + total_body_len {
                    let mmap = unsafe {
                        MmapOptions::new()
                            .offset(0)
                            .len(flen)
                            .map(&*file)
                            .map_err(|e| anyhow!("bloom mmap: {}", e))?
                    };
                    mmap_opt = Some(mmap);
                } else {
                    // fallback: читаем body в RAM
                    body_opt = Some(read_body_from_path(&path, hdr_size_u64, total_body_len)?);
                }
            } else {
                body_opt = Some(read_body_from_path(&path, hdr_size_u64, total_body_len)?);
            }
        } else if total_body_len > 0 {
            // RAM режим
            body_opt = Some(read_body_from_path(&path, hdr_size_u64, total_body_len)?);
        }

        Ok(Self {
            root: root.to_path_buf(),
            path,
            meta: BloomMeta {
                buckets,
                bytes_per_bucket,
                k_hashes,
                seed1,
                seed2,
                last_lsn,
            },
            hdr_size_u64,
            hdr_size_usize,
            version,
            f_ro,
            body: body_opt,
            mmap: mmap_opt,
        })
    }

    /// Создать новый bloom.bin (v2).
    pub fn create(root: &Path, meta: BloomMeta) -> Result<Self> {
        if meta.bytes_per_bucket == 0 || meta.k_hashes == 0 {
            return Err(anyhow!("bytes_per_bucket and k_hashes must be > 0"));
        }
        let path = root.join("bloom.bin");
        if path.exists() {
            return Err(anyhow!("bloom sidecar already exists at {}", path.display()));
        }

        let mut f = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("create bloom {}", path.display()))?;

        // Header v2
        let mut hdr = vec![0u8; HDR_SIZE_V2_USIZE];
        hdr[OFF_MAGIC..OFF_MAGIC + 8].copy_from_slice(MAGIC);
        LittleEndian::write_u32(&mut hdr[OFF_VERSION..OFF_VERSION + 4], VERSION_V2);
        LittleEndian::write_u32(&mut hdr[OFF_BUCKETS..OFF_BUCKETS + 4], meta.buckets);
        LittleEndian::write_u32(&mut hdr[OFF_BYTES_PER_BUCKET..OFF_BYTES_PER_BUCKET + 4], meta.bytes_per_bucket);
        LittleEndian::write_u32(&mut hdr[OFF_K_HASHES..OFF_K_HASHES + 4], meta.k_hashes);
        LittleEndian::write_u64(&mut hdr[OFF_SEED1..OFF_SEED1 + 8], meta.seed1);
        LittleEndian::write_u64(&mut hdr[OFF_SEED2..OFF_SEED2 + 8], meta.seed2);
        LittleEndian::write_u64(&mut hdr[OFF_LAST_LSN..OFF_LAST_LSN + 8], meta.last_lsn);
        f.seek(SeekFrom::Start(0))?;
        f.write_all(&hdr)?;

        // Body: zeros
        let total = meta.buckets as u64 * meta.bytes_per_bucket as u64;
        let chunk = vec![0u8; 8192];
        let mut written = 0u64;
        while written < total {
            let to_write = std::cmp::min(chunk.len() as u64, total - written) as usize;
            f.write_all(&chunk[..to_write])?;
            written += to_write as u64;
        }
        let _ = f.sync_all();

        // RO‑handle и view
        let f_ro = match OpenOptions::new().read(true).open(&path) {
            Ok(file) => Some(Mutex::new(file)),
            Err(_) => None,
        };

        let total_body_len = (meta.buckets as usize).saturating_mul(meta.bytes_per_bucket as usize);

        let mut body_opt: Option<Box<[u8]>> = None;
        let mut mmap_opt: Option<Mmap> = None;

        if bloom_mmap_enabled() && total_body_len > 0 {
            if let Some(ro) = &f_ro {
                let file = ro.lock().map_err(|_| anyhow!("bloom ro handle poisoned"))?;
                // Новый безопасный режим: мапим ВЕСЬ файл от offset=0
                let flen = file.metadata()?.len() as usize;
                if flen >= HDR_SIZE_V2_USIZE + total_body_len {
                    let mmap = unsafe {
                        MmapOptions::new()
                            .offset(0)
                            .len(flen)
                            .map(&*file)
                            .map_err(|e| anyhow!("bloom mmap: {}", e))?
                    };
                    mmap_opt = Some(mmap);
                } else {
                    body_opt = Some(read_body_from_path(&path, HDR_SIZE_V2_U64, total_body_len)?);
                }
            } else {
                body_opt = Some(read_body_from_path(&path, HDR_SIZE_V2_U64, total_body_len)?);
            }
        } else if total_body_len > 0 {
            body_opt = Some(read_body_from_path(&path, HDR_SIZE_V2_U64, total_body_len)?);
        }

        Ok(Self {
            root: root.to_path_buf(),
            path,
            meta,
            hdr_size_u64: HDR_SIZE_V2_U64,
            hdr_size_usize: HDR_SIZE_V2_USIZE,
            version: VERSION_V2,
            f_ro,
            body: body_opt,
            mmap: mmap_opt,
        })
    }

    /// Открыть или создать bloom.bin (v2) под параметры по умолчанию.
    pub fn open_or_create_for_db(db: &Db, bytes_per_bucket: u32, k_hashes: u32) -> Result<Self> {
        let root = &db.root;
        let dir_buckets = db.dir.bucket_count;
        let default_meta = BloomMeta {
            buckets: dir_buckets,
            bytes_per_bucket,
            k_hashes,
            seed1: 0x9E37_79B9_7F4A_7C15,
            seed2: 0xC2B2_AE3D_27D4_EB4F,
            last_lsn: db.pager.meta.last_lsn,
        };
        let path = root.join("bloom.bin");
        if path.exists() {
            BloomSidecar::open(root)
        } else {
            BloomSidecar::create(root, default_meta)
        }
    }
}