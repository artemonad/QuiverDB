//! bloom/sidecar — реализация BloomSidecar (P2BLM01) + интеграция с глобальным кэшем.
//!
//! Формат (LE):
//! - Header (48 B) v2:
//!   [magic8="P2BLM01\0"]
//!   [version u32=2]
//!   [buckets u32]
//!   [bytes_per_bucket u32]
//!   [k_hashes u32]
//!   [seed1 u64]
//!   [seed2 u64]
//!   [last_lsn u64]
//! - Body: buckets × bytes_per_bucket (последовательно).
//!
//! - last_lsn в заголовке должен совпадать с meta.last_lsn БД, чтобы отрицательные хинты были безопасны.
//!
//! Consistency:
//! - Любые изменения bloom.bin выполняются под эксклюзивной блокировкой <root>/bloom.bin.lock,
//!   затем sync_all().
//!
//! Perf:
//! - RAM‑режим (по умолчанию): загружаем весь body в память один раз и тестируем биты напрямую.
//! - MMAP‑режим (ENV P1_BLOOM_MMAP=1): memory‑map на body; test() читает из mmap.
//! - Оба режима полностью исключают I/O на hot‑path exists/get_miss. Есть fallback на LRU/file.

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use fs2::FileExt;
use memmap2::{Mmap, MmapOptions};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::db::core::Db;
use crate::dir::NO_PAGE;
use crate::page::{kv_header_read_v3, kv_read_record, PAGE_MAGIC, PAGE_TYPE_KV_RH3, KV_HDR_MIN};
// packed-aware обход всех записей страницы (reverse слоты, новые → старые)
use crate::page::kv::kv_for_each_record;

// Глобальный LRU-кэш битов (fallback)
use super::cache::{bloom_cache_get, bloom_cache_put};

// -------------------- константы формата --------------------

const MAGIC: &[u8; 8] = b"P2BLM01\0";
const VERSION_V1: u32 = 1;
const VERSION_V2: u32 = 2;

// Размер заголовка
const HDR_SIZE_V1_USIZE: usize = 40;
const HDR_SIZE_V1_U64: u64 = 40;
const HDR_SIZE_V2_USIZE: usize = 48;
const HDR_SIZE_V2_U64: u64 = 48;

// Смещения v2 (первые поля совпадают с v1)
const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 8;
const OFF_BUCKETS: usize = 12;
const OFF_BYTES_PER_BUCKET: usize = 16;
const OFF_K_HASHES: usize = 20;
const OFF_SEED1: usize = 24;
const OFF_SEED2: usize = 32;
const OFF_LAST_LSN: usize = 40;

// -------------------- ENV toggles --------------------

fn bloom_mmap_enabled() -> bool {
    std::env::var("P1_BLOOM_MMAP")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

fn bloom_lock_path(root: &Path) -> PathBuf {
    root.join("bloom.bin.lock")
}

fn lock_bloom_file(root: &Path) -> Result<File> {
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

// -------------------- структура метаданных --------------------

/// Параметры Bloom-файла (метаданные).
#[derive(Debug, Clone)]
pub struct BloomMeta {
    pub buckets: u32,
    pub bytes_per_bucket: u32,
    pub k_hashes: u32,
    pub seed1: u64,
    pub seed2: u64,
    pub last_lsn: u64, // v2: LSN консистентности; 0 для v1
}

/// Хэндл для работы с <root>/bloom.bin.
#[derive(Debug)]
pub struct BloomSidecar {
    pub(crate) root: PathBuf,
    pub(crate) path: PathBuf,
    meta: BloomMeta,
    hdr_size_u64: u64,   // 40 (v1) или 48 (v2)
    hdr_size_usize: usize,
    version: u32,

    // Постоянный RO‑хэндл (для mmap и fallback чтений)
    f_ro: Option<Mutex<File>>,

    // RAM‑копия body (если выбрали RAM режим)
    body: Option<Box<[u8]>>,

    // MMAP view на body (если выбрали mmap режим)
    mmap: Option<Mmap>,
}

impl BloomSidecar {
    /// Открыть существующий bloom.bin (RO).
    pub fn open_ro(root: &Path) -> Result<Self> {
        Self::open_inner(root, false)
    }

    /// Открыть существующий bloom.bin (RW).
    pub fn open(root: &Path) -> Result<Self> {
        Self::open_inner(root, true)
    }

    fn open_inner(root: &Path, write: bool) -> Result<Self> {
        let path = root.join("bloom.bin");
        let mut opts = OpenOptions::new();
        opts.read(true);
        if write {
            opts.write(true);
        }
        let mut f = opts
            .open(&path)
            .with_context(|| format!("open bloom {}", path.display()))?;

        // Header
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

        let mut hdr = vec![0u8; hdr_size_usize];
        f.seek(SeekFrom::Start(0))?;
        f.read_exact(&mut hdr)?;

        let buckets = LittleEndian::read_u32(&hdr[OFF_BUCKETS..OFF_BUCKETS + 4]);
        let bytes_per_bucket =
            LittleEndian::read_u32(&hdr[OFF_BYTES_PER_BUCKET..OFF_BYTES_PER_BUCKET + 4]);
        let k_hashes = LittleEndian::read_u32(&hdr[OFF_K_HASHES..OFF_K_HASHES + 4]);
        let seed1 = LittleEndian::read_u64(&hdr[OFF_SEED1..OFF_SEED1 + 8]);
        let seed2 = LittleEndian::read_u64(&hdr[OFF_SEED2..OFF_SEED2 + 8]);
        let last_lsn = if version == VERSION_V2 {
            LittleEndian::read_u64(&hdr[OFF_LAST_LSN..OFF_LAST_LSN + 8])
        } else {
            0
        };

        // Подготовим ro‑handle
        let f_ro = match OpenOptions::new().read(true).open(&path) {
            Ok(file) => Some(Mutex::new(file)),
            Err(_) => None,
        };

        // Выбор режима: mmap или RAM
        let total_body_len = (buckets as usize).saturating_mul(bytes_per_bucket as usize);

        let mut body_opt: Option<Box<[u8]>> = None;
        let mut mmap_opt: Option<Mmap> = None;

        if bloom_mmap_enabled() && total_body_len > 0 {
            if let Some(ro) = &f_ro {
                let file = ro.lock().map_err(|_| anyhow!("bloom ro handle poisoned"))?;
                // offset = hdr_size_u64, len = total_body_len
                let mmap = unsafe {
                    MmapOptions::new()
                        .offset(hdr_size_u64)
                        .len(total_body_len)
                        .map(&*file)
                        .map_err(|e| anyhow!("bloom mmap: {}", e))?
                };
                mmap_opt = Some(mmap);
            } else {
                // нет ro‑handle — fallback на RAM
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

        // Поднимем RO‑handle и сформируем view (mmap/RAM)
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
                let mmap = unsafe {
                    MmapOptions::new()
                        .offset(HDR_SIZE_V2_U64)
                        .len(total_body_len)
                        .map(&*file)
                        .map_err(|e| anyhow!("bloom mmap: {}", e))?
                };
                mmap_opt = Some(mmap);
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

    /// Полная перестройка Bloom-файла по всей БД (синхронно).
    pub fn rebuild_all(&mut self, db: &Db) -> Result<()> {
        for b in 0..db.dir.bucket_count {
            self.rebuild_bucket(db, b)?;
        }
        self.set_last_lsn(db.pager.meta.last_lsn)?;
        // Обновим view
        self.reload_views()?;
        Ok(())
    }

    /// Перестроить Bloom-биты для одного бакета.
    pub fn rebuild_bucket(&mut self, db: &Db, bucket: u32) -> Result<()> {
        if bucket >= self.meta.buckets {
            return Err(anyhow!(
                "bucket {} out of range 0..{}",
                bucket,
                self.meta.buckets - 1
            ));
        }
        let ps = db.pager.meta.page_size as usize;
        let now = now_secs();

        enum State { Selected, Deleted }
        let mut state: HashMap<Vec<u8>, State> = HashMap::new();

        let mut page_buf = vec![0u8; ps];

        let mut pid = db.dir.head(bucket)?;
        while pid != NO_PAGE {
            db.pager.read_page(pid, &mut page_buf)?;
            if &page_buf[0..4] != PAGE_MAGIC {
                break;
            }
            let ptype = LittleEndian::read_u16(&page_buf[6..8]);
            if ptype != PAGE_TYPE_KV_RH3 {
                break;
            }
            let h = kv_header_read_v3(&page_buf)?;

            let mut touched = false;
            kv_for_each_record(&page_buf, |k, _v, expires_at_sec, vflags| {
                touched = true;
                if state.contains_key(k) { return; }
                let is_tomb = (vflags & 0x1) == 1;
                let ttl_ok = expires_at_sec == 0 || now < expires_at_sec;
                if is_tomb { state.insert(k.to_vec(), State::Deleted); }
                else if ttl_ok { state.insert(k.to_vec(), State::Selected); }
            });

            if !touched {
                if let Some((k, _v, expires_at_sec, vflags)) = kv_read_record(&page_buf, KV_HDR_MIN) {
                    if !state.contains_key(k) {
                        let is_tomb = (vflags & 0x1) == 1;
                        let ttl_ok = expires_at_sec == 0 || now < expires_at_sec;
                        if is_tomb { state.insert(k.to_vec(), State::Deleted); }
                        else if ttl_ok { state.insert(k.to_vec(), State::Selected); }
                    }
                }
            }

            pid = h.next_page_id;
        }

        // Сформируем биты
        let mut bits = vec![0u8; self.meta.bytes_per_bucket as usize];
        for (k, st) in state.into_iter() {
            if let State::Selected = st {
                self.add_key_to_bits(&k, &mut bits);
            }
        }

        // Запись под lock
        let _lk = lock_bloom_file(&self.root)?;
        let mut f = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let off = self.hdr_size_u64 + (bucket as u64) * (self.meta.bytes_per_bucket as u64);
        f.seek(SeekFrom::Start(off))?;
        f.write_all(&bits)?;
        let _ = f.sync_all();

        Ok(())
    }

    /// Тест по битам (из RAM/mmap/LRU/file).
    pub fn test(&self, bucket: u32, key: &[u8]) -> Result<bool> {
        if bucket >= self.meta.buckets {
            return Err(anyhow!(
                "bucket {} out of range 0..{}",
                bucket,
                self.meta.buckets - 1
            ));
        }

        // RAM
        if let Some(ref body) = self.body {
            let bpb = self.meta.bytes_per_bucket as usize;
            let start = (bucket as usize) * bpb;
            let end = start + bpb;
            if end <= body.len() {
                return Ok(self.test_in_bits(key, &body[start..end]));
            }
        }

        // MMAP
        if let Some(ref mm) = self.mmap {
            let bpb = self.meta.bytes_per_bucket as usize;
            let start = (bucket as usize) * bpb;
            let end = start + bpb;
            if end <= mm.len() {
                return Ok(self.test_in_bits(key, &mm[start..end]));
            }
        }

        // LRU
        if let Some(bits) = bloom_cache_get(&self.path, bucket, self.meta.last_lsn) {
            return Ok(self.test_in_bits(key, &bits));
        }

        // File fallback
        let mut bits = vec![0u8; self.meta.bytes_per_bucket as usize];
        let off = self.hdr_size_u64 + (bucket as u64) * (self.meta.bytes_per_bucket as u64);

        if let Some(ref mtx) = self.f_ro {
            let mut f = mtx.lock().map_err(|_| anyhow!("bloom ro handle poisoned"))?;
            f.seek(SeekFrom::Start(off))?;
            f.read_exact(&mut bits)?;
        } else {
            let mut f = OpenOptions::new().read(true).open(&self.path)?;
            f.seek(SeekFrom::Start(off))?;
            f.read_exact(&mut bits)?;
        }

        bloom_cache_put(&self.path, bucket, self.meta.last_lsn, bits.clone());
        Ok(self.test_in_bits(key, &bits))
    }

    /// Обновить last_lsn (v2) — под lock + перезагрузка view.
    pub fn set_last_lsn(&mut self, last_lsn: u64) -> Result<()> {
        if self.version != VERSION_V2 {
            self.meta.last_lsn = 0;
            return Ok(());
        }
        let _lk = lock_bloom_file(&self.root)?;
        let mut f = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.write_header_last_lsn_locked(&mut f, last_lsn)?;
        let _ = f.sync_all();
        self.meta.last_lsn = last_lsn;
        self.reload_views()?;
        Ok(())
    }

    /// Проверка свежести Bloom по last_lsn БД.
    pub fn is_fresh_for_db(&self, db: &Db) -> bool {
        self.version == VERSION_V2 && self.meta.last_lsn == db.pager.meta.last_lsn
    }

    /// Количество бакетов.
    pub fn buckets(&self) -> u32 { self.meta.buckets }
    /// Байт на бакет.
    pub fn bytes_per_bucket(&self) -> u32 { self.meta.bytes_per_bucket }
    /// Число хэшей.
    pub fn k_hashes(&self) -> u32 { self.meta.k_hashes }
    /// last_lsn из заголовка bloom (0 для v1).
    pub fn last_lsn(&self) -> u64 { self.meta.last_lsn }

    /// Delta‑update API: обновить биты бакета (keys) и last_lsn, затем обновить view.
    pub fn update_bucket_bits(&mut self, bucket: u32, keys: &[&[u8]], new_last_lsn: u64) -> Result<()> {
        if bucket >= self.meta.buckets {
            return Err(anyhow!(
                "bucket {} out of range 0..{}",
                bucket,
                self.meta.buckets - 1
            ));
        }

        // lock на время RMW
        let _lk = lock_bloom_file(&self.root)?;
        let mut f = OpenOptions::new().read(true).write(true).open(&self.path)?;

        // Read current bits
        let mut bits = vec![0u8; self.meta.bytes_per_bucket as usize];
        let off = self.hdr_size_u64 + (bucket as u64) * (self.meta.bytes_per_bucket as u64);
        f.seek(SeekFrom::Start(off))?;
        f.read_exact(&mut bits)?;

        // Apply keys
        for k in keys {
            if !k.is_empty() {
                self.add_key_to_bits(k, &mut bits);
            }
        }

        // Write bits
        f.seek(SeekFrom::Start(off))?;
        f.write_all(&bits)?;
        let _ = f.sync_all();

        // Update header.last_lsn (v2)
        if self.version == VERSION_V2 {
            self.write_header_last_lsn_locked(&mut f, new_last_lsn)?;
            let _ = f.sync_all();
            self.meta.last_lsn = new_last_lsn;
        }

        // Обновим RAM/mmap view
        self.reload_views()?;

        // LRU — на новый LSN
        bloom_cache_put(&self.path, bucket, self.meta.last_lsn, bits);

        Ok(())
    }

    // ---------- внутренние помощники ----------

    fn write_header_last_lsn_locked(&mut self, f: &mut File, last_lsn: u64) -> Result<()> {
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

    fn reload_views(&mut self) -> Result<()> {
        let total_body_len = (self.meta.buckets as usize)
            .saturating_mul(self.meta.bytes_per_bucket as usize);

        // Если включен mmap — ремапим, иначе обновляем RAM
        if bloom_mmap_enabled() {
            // Нужен ro‑handle
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
                // RAM сбросим, если был
                self.body = None;
            } else {
                // нет ro‑handle — fallback на RAM
                self.body = Some(read_body_from_path(&self.path, self.hdr_size_u64, total_body_len)?);
                self.mmap = None;
            }
        } else {
            // RAM — перечитаем
            self.body = Some(read_body_from_path(&self.path, self.hdr_size_u64, total_body_len)?);
            // mmap сбросим
            self.mmap = None;
        }
        Ok(())
    }

    fn add_key_to_bits(&self, key: &[u8], bits: &mut [u8]) {
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

    fn test_in_bits(&self, key: &[u8], bits: &[u8]) -> bool {
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
}

// -------------------- helpers --------------------

fn read_body_from_path(path: &Path, offset: u64, len: usize) -> Result<Box<[u8]>> {
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

#[inline]
fn now_secs() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_secs() as u64).min(u32::MAX as u64) as u32
}