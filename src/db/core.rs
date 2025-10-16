//! db/core — ядро high-level API: структура Db, init(), базовые утилиты lock
//!
//! Дополнено:
//! - impl Drop for Db: в writer-режиме при закрытии усечёт WAL до заголовка и
//!   сохранит актуальную meta (last_lsn/next_page_id + clean_shutdown=true) через write_meta_overwrite.
//! - In‑memory keydir (per‑bucket key -> (page_id, off)) для ускорения get()/exists/scan без изменения форматов.
//! - Writer-only обёртки для обновления голов каталога:
//!   Db::set_dir_head / Db::set_dir_heads_bulk (используйте в тестах/админ‑скриптах).
//!   Прямой вызов Directory::set_head/set_heads_bulk теперь закрыт во внешнем API (pub(crate)).

use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
// NEW: постоянный RO-хэндл bloom.bin (ускорение get_miss/exists_miss)
use std::sync::Arc;

use crate::dir::Directory;
use crate::meta::{init_meta_v4, write_meta_overwrite};
use crate::pager::Pager;
// NEW: импорт BloomSidecar для поля Db
use crate::bloom::BloomSidecar;

pub(crate) const LOCK_FILE: &str = "LOCK";

pub(crate) fn open_lock_file(root: &Path) -> Result<std::fs::File> {
    let p = root.join(LOCK_FILE);
    let f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&p)
        .with_context(|| format!("open lock file {}", p.display()))?;
    Ok(f)
}

/// Локация ключа для in‑memory keydir: (page_id, off).
/// pid = NO_PAGE означает tombstone (ключ отсутствует).
#[derive(Clone, Copy, Debug)]
pub struct MemKeyLoc {
    pub pid: u64,
    pub off: u32,
}

/// Простой in‑memory keydir: per‑bucket map key -> (page_id, off).
/// Назначение: ускорить get()/exists/scan без изменения форматов.
/// Построение выполняется отдельно (на этапе открытия RO/по команде).
#[derive(Debug, Default)]
pub struct MemKeyDir {
    // buckets == maps.len()
    maps: Vec<HashMap<Vec<u8>, MemKeyLoc>>,
}

impl MemKeyDir {
    pub fn new(buckets: u32) -> Self {
        let mut maps = Vec::with_capacity(buckets as usize);
        for _ in 0..buckets {
            maps.push(HashMap::new());
        }
        Self { maps }
    }

    #[inline]
    pub fn buckets(&self) -> u32 {
        self.maps.len() as u32
    }

    /// Вставка локации: (pid, off). pid=NO_PAGE означает tombstone.
    #[inline]
    pub fn insert_loc(&mut self, bucket: u32, key: &[u8], loc: MemKeyLoc) {
        if (bucket as usize) < self.maps.len() {
            self.maps[bucket as usize].insert(key.to_vec(), loc);
        }
    }

    /// Вставка совместимости: только pid (off=0).
    #[inline]
    pub fn insert(&mut self, bucket: u32, key: &[u8], page_id: u64) {
        self.insert_loc(bucket, key, MemKeyLoc { pid: page_id, off: 0 });
    }

    /// Получить локацию (pid, off).
    #[inline]
    pub fn get_loc(&self, bucket: u32, key: &[u8]) -> Option<MemKeyLoc> {
        if (bucket as usize) < self.maps.len() {
            self.maps[bucket as usize].get(key).copied()
        } else {
            None
        }
    }

    /// Совместимый геттер: вернуть только pid (или None).
    #[inline]
    pub fn get(&self, bucket: u32, key: &[u8]) -> Option<u64> {
        self.get_loc(bucket, key).map(|l| l.pid)
    }

    #[inline]
    pub fn clear(&mut self) {
        for m in &mut self.maps {
            m.clear();
        }
    }

    /// Обход всех пар (bucket, key, pid). NO_PAGE не фильтруется — решает вызывающий код.
    #[inline]
    pub fn for_each<F: FnMut(u32, &[u8], u64)>(&self, mut f: F) {
        for (b, map) in self.maps.iter().enumerate() {
            let b = b as u32;
            for (k, loc) in map.iter() {
                f(b, k.as_slice(), loc.pid);
            }
        }
    }

    /// Обход по префиксу (bucket, key, pid).
    #[inline]
    pub fn for_each_prefix<F: FnMut(u32, &[u8], u64)>(&self, prefix: &[u8], mut f: F) {
        for (b, map) in self.maps.iter().enumerate() {
            let b = b as u32;
            for (k, loc) in map.iter() {
                if k.as_slice().starts_with(prefix) {
                    f(b, k.as_slice(), loc.pid);
                }
            }
        }
    }

    /// Обход всех пар (bucket, key, pid, off).
    #[inline]
    pub fn for_each_with_off<F: FnMut(u32, &[u8], MemKeyLoc)>(&self, mut f: F) {
        for (b, map) in self.maps.iter().enumerate() {
            let b = b as u32;
            for (k, loc) in map.iter() {
                f(b, k.as_slice(), *loc);
            }
        }
    }

    /// Обход по префиксу (bucket, key, pid, off).
    #[inline]
    pub fn for_each_prefix_with_off<F: FnMut(u32, &[u8], MemKeyLoc)>(&self, prefix: &[u8], mut f: F) {
        for (b, map) in self.maps.iter().enumerate() {
            let b = b as u32;
            for (k, loc) in map.iter() {
                if k.as_slice().starts_with(prefix) {
                    f(b, k.as_slice(), *loc);
                }
            }
        }
    }
}

pub struct Db {
    pub root: PathBuf,
    pub pager: Pager,
    pub dir: Directory,
    pub(crate) _lock: std::fs::File, // держим дескриптор
    pub(crate) readonly: bool,

    // optional in‑memory keydir (RO‑ускорение get/exists/scan). Если Some — используем.
    pub(crate) mem_keydir: Option<MemKeyDir>,

    // NEW: кэшированный RO‑хэндл bloom.bin (для тестов exists/get-miss без лишних open()).
    // Заполняется в open_ro_*; в writer-режиме обычно None.
    pub(crate) bloom_ro: Option<Arc<BloomSidecar>>,
}

impl Db {
    pub fn init(root: &Path, page_size: u32, buckets: u32) -> Result<()> {
        if !root.exists() {
            std::fs::create_dir_all(root)
                .with_context(|| format!("create root {}", root.display()))?;
        }
        init_meta_v4(
            root,
            page_size,
            crate::meta::HASH_KIND_XX64_SEED0,
            crate::meta::CODEC_NONE,
            crate::meta::CKSUM_CRC32C,
        )?;
        Directory::create(root, buckets)?;
        Ok(())
    }

    // -------- in‑memory keydir helpers --------

    #[inline]
    pub(crate) fn ensure_mem_keydir(&mut self) {
        if self.mem_keydir.is_none() {
            let buckets = self.dir.bucket_count;
            self.mem_keydir = Some(MemKeyDir::new(buckets));
        }
    }

    /// Публичный геттер: присутствует ли in‑memory keydir (ускоритель RO-сканов и get/exists).
    #[inline]
    pub fn has_mem_keydir(&self) -> bool {
        self.mem_keydir.is_some()
    }

    /// Новый быстрый путь: получить локацию (pid, off).
    #[inline]
    pub(crate) fn mem_keydir_get_loc(&self, bucket: u32, key: &[u8]) -> Option<MemKeyLoc> {
        self.mem_keydir.as_ref().and_then(|kd| kd.get_loc(bucket, key))
    }

    /// Вставка локации (pid, off) — используется строителем keydir.
    #[inline]
    pub(crate) fn mem_keydir_insert_loc(&mut self, bucket: u32, key: &[u8], loc: MemKeyLoc) {
        if let Some(kd) = self.mem_keydir.as_mut() {
            kd.insert_loc(bucket, key, loc);
        }
    }

    #[inline]
    pub(crate) fn mem_keydir_clear(&mut self) {
        if let Some(kd) = self.mem_keydir.as_mut() {
            kd.clear();
        }
    }

    /// Обход всех пар (bucket, key, pid). NO_PAGE не фильтруется — решает вызывающий код.
    #[inline]
    pub(crate) fn mem_keydir_for_each<F: FnMut(u32, &[u8], u64)>(&self, f: F) {
        if let Some(kd) = self.mem_keydir.as_ref() {
            kd.for_each(f);
        }
    }

    /// Обход по префиксу (bucket, key, pid).
    #[inline]
    pub(crate) fn mem_keydir_for_each_prefix<F: FnMut(u32, &[u8], u64)>(&self, prefix: &[u8], f: F) {
        if let Some(kd) = self.mem_keydir.as_ref() {
            kd.for_each_prefix(prefix, f);
        }
    }

    /// Обход с оффсетами (bucket, key, MemKeyLoc).
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn mem_keydir_for_each_with_off<F: FnMut(u32, &[u8], MemKeyLoc)>(&self, f: F) {
        if let Some(kd) = self.mem_keydir.as_ref() {
            kd.for_each_with_off(f);
        }
    }

    /// Обход по префиксу с оффсетами (bucket, key, MemKeyLoc).
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn mem_keydir_for_each_prefix_with_off<F: FnMut(u32, &[u8], MemKeyLoc)>(&self, prefix: &[u8], f: F) {
        if let Some(kd) = self.mem_keydir.as_ref() {
            kd.for_each_prefix_with_off(prefix, f);
        }
    }

    // -------- writer-only directory head updates (public wrappers) --------

    /// Writer-only: установить голову бакета напрямую (админ/тестовые задачи).
    /// Атомарно обновляет shard (tmp+rename с CRC). Возвращает Err в RO-режиме.
    pub fn set_dir_head(&mut self, bucket: u32, page_id: u64) -> Result<()> {
        if self.readonly {
            return Err(anyhow!("set_dir_head: Db is read-only (writer-only op)"));
        }
        self.dir.set_head(bucket, page_id)
    }

    /// Writer-only: атомарно обновить несколько голов за один проход.
    /// Возвращает Err в RO-режиме.
    pub fn set_dir_heads_bulk(&mut self, updates: &[(u32, u64)]) -> Result<()> {
        if self.readonly {
            return Err(anyhow!("set_dir_heads_bulk: Db is read-only (writer-only op)"));
        }
        self.dir.set_heads_bulk(updates)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // Только для writer'а (эксклюзивный режим).
        if self.readonly {
            return;
        }

        // 1) Усечём WAL до заголовка (идемпотентно). Ошибки игнорируем в Drop.
        let _ = (|| -> anyhow::Result<()> {
            let mut wal = crate::wal::Wal::open_for_append(&self.root)?;
            wal.truncate_to_header()?;
            Ok(())
        })();

        // 2) Сохранить актуальную meta (next_page_id / last_lsn) и clean_shutdown=true (best-effort).
        let _ = (|| -> anyhow::Result<()> {
            let mut m = self.pager.meta.clone();
            m.clean_shutdown = true;
            write_meta_overwrite(&self.root, &m)
        })();

        // Примечание: дескриптор LOCK освободится автоматически после Drop,
        // порядок вызовов гарантирован: сначала этот Drop, затем поля.
    }
}