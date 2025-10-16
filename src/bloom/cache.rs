//! bloom/cache — глобальный LRU‑кэш битовых массивов Bloom-фильтра (per bucket).
//!
//! Назначение: снизить I/O при интенсивных exists()/get(not-found) с Bloom fast‑path.
//! Ключ кэша = (db_id u64, bucket u32, last_lsn u64).
//!
//! Изменения:
//! - Ключ db_id теперь кэшируется по КАНОНИЗИРОВАННОМУ пути (std::fs::canonicalize).
//!   Это устраняет дубли для разных представлений одного и того же пути (относительные/абсолютные, symlink).
//!
//! Управление ёмкостью: ENV P1_BLOOM_CACHE_BUCKETS (по умолчанию 8; 0 — выключено).
//!
//! Публичный API:
//! - bloom_cache_get(path, bucket, last_lsn) -> Option<Vec<u8>>
//! - bloom_cache_put(path, bucket, last_lsn, bits)
//! - bloom_cache_stats() -> (capacity, entries)
//! - bloom_cache_counters() -> (hits, misses)

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct BloomCacheKey {
    db_id: u64,
    bucket: u32,
    last_lsn: u64,
}

// --------- быстрый кэш db_id для путей (по канонизированному пути) ----------

static PATH_DBID_CACHE: OnceLock<Mutex<HashMap<PathBuf, u64>>> = OnceLock::new();

fn db_id_for_path(path: &Path) -> u64 {
    // Канонизируем путь, чтобы все формы одного и того же пути (rel/abs/symlink) имели единый ключ
    let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());

    if let Ok(mut guard) = PATH_DBID_CACHE
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
    {
        if let Some(&v) = guard.get(&canon) {
            return v;
        }
        // Вычислим впервые и положим в кэш
        let v = compute_db_id_path(&canon);
        guard.insert(canon, v);
        return v;
    }
    // В редком случае (poisoned lock) вычислим напрямую
    compute_db_id_path(&canon)
}

fn compute_db_id_path(path: &Path) -> u64 {
    use std::hash::Hasher;
    // Путь здесь уже канонизирован снаружи, но повторная попытка не повредит (на случай прямого вызова)
    let canon = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let s = canon.to_string_lossy();
    let mut h = twox_hash::XxHash64::with_seed(0);
    h.write(s.as_bytes());
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(md) = std::fs::metadata(&canon) {
            h.write_u64(md.dev() as u64);
            h.write_u64(md.ino() as u64);
        }
    }
    h.finish()
}

// -------------------- LRU структура --------------------

struct Node {
    value: Vec<u8>,
    prev: Option<BloomCacheKey>,
    next: Option<BloomCacheKey>,
}

struct BloomCache {
    // Настройки
    cap: usize,
    inited: bool,

    // Основные структуры
    map: HashMap<BloomCacheKey, Node>,
    head: Option<BloomCacheKey>, // LRU (самый старый)
    tail: Option<BloomCacheKey>, // MRU (самый новый)

    // Счётчики (для метрик)
    hits: u64,
    misses: u64,
}

impl BloomCache {
    fn new() -> Self {
        Self {
            cap: 0,
            inited: false,
            map: HashMap::new(),
            head: None,
            tail: None,
            hits: 0,
            misses: 0,
        }
    }

    fn init_if_needed(&mut self) {
        if self.inited {
            return;
        }
        let cap = std::env::var("P1_BLOOM_CACHE_BUCKETS")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(8);
        self.cap = cap;
        self.inited = true;
    }

    #[inline]
    fn enabled(&self) -> bool {
        self.cap > 0
    }

    // ---- двусвязный список: helpers ----

    fn detach(&mut self, key: &BloomCacheKey) {
        // Прочитаем prev/next через неизменяемый доступ
        let (prev, next) = match self.map.get(key) {
            Some(n) => (n.prev, n.next),
            None => return,
        };

        // Обновим prev.next
        match prev {
            Some(p) => {
                if let Some(n) = self.map.get_mut(&p) {
                    n.next = next;
                }
            }
            None => {
                // key был head
                self.head = next;
            }
        }

        // Обновим next.prev
        match next {
            Some(nk) => {
                if let Some(n) = self.map.get_mut(&nk) {
                    n.prev = prev;
                }
            }
            None => {
                // key был tail
                self.tail = prev;
            }
        }

        // Обнулим ссылки у самого узла
        if let Some(node) = self.map.get_mut(key) {
            node.prev = None;
            node.next = None;
        }
    }

    fn push_back(&mut self, key: BloomCacheKey) {
        match self.tail {
            None => {
                self.head = Some(key);
                self.tail = Some(key);
            }
            Some(tk) => {
                {
                    if let Some(tail_node) = self.map.get_mut(&tk) {
                        tail_node.next = Some(key);
                    }
                }
                {
                    if let Some(new_node) = self.map.get_mut(&key) {
                        new_node.prev = Some(tk);
                        new_node.next = None;
                    }
                }
                self.tail = Some(key);
            }
        }
    }

    fn move_to_back(&mut self, key: &BloomCacheKey) {
        if self.tail.as_ref() == Some(key) {
            return;
        }
        self.detach(key);
        if self.map.contains_key(key) {
            self.push_back(*key);
        }
    }

    fn pop_front(&mut self) -> Option<(BloomCacheKey, Vec<u8>)> {
        let head_key = self.head?;
        let mut val = Vec::new();

        if let Some(mut node) = self.map.remove(&head_key) {
            val = node.value;
            let next = node.next.take();
            match next {
                Some(nk) => {
                    if let Some(n) = self.map.get_mut(&nk) {
                        n.prev = None;
                    }
                    self.head = Some(nk);
                }
                None => {
                    self.head = None;
                    self.tail = None;
                }
            }
        }
        Some((head_key, val))
    }

    // ---- API ----

    fn get(&mut self, key: &BloomCacheKey) -> Option<Vec<u8>> {
        if !self.enabled() {
            return None;
        }
        if self.map.contains_key(key) {
            self.hits = self.hits.saturating_add(1);
            self.move_to_back(key);
            return self.map.get(key).map(|n| n.value.clone());
        }
        self.misses = self.misses.saturating_add(1);
        None
    }

    fn put(&mut self, key: BloomCacheKey, bits: Vec<u8>) {
        if !self.enabled() {
            return;
        }
        if let Some(n) = self.map.get_mut(&key) {
            n.value = bits;
            self.move_to_back(&key);
            return;
        }
        while self.map.len() >= self.cap {
            let _ = self.pop_front();
        }
        self.map.insert(
            key,
            Node {
                value: bits,
                prev: None,
                next: None,
            },
        );
        self.push_back(key);
    }

    fn stats(&mut self) -> (usize, usize) {
        self.init_if_needed();
        (self.cap, self.map.len())
    }

    fn counters(&mut self) -> (u64, u64) {
        self.init_if_needed();
        (self.hits, self.misses)
    }
}

static BLOOM_CACHE: OnceLock<Mutex<BloomCache>> = OnceLock::new();

#[inline]
fn cache_lock() -> &'static Mutex<BloomCache> {
    BLOOM_CACHE.get_or_init(|| Mutex::new(BloomCache::new()))
}

/// Получить биты бакета из кэша, если есть.
pub fn bloom_cache_get(path: &Path, bucket: u32, last_lsn: u64) -> Option<Vec<u8>> {
    let mut cg = cache_lock().lock().ok()?;
    cg.init_if_needed();
    if !cg.enabled() {
        return None;
    }
    let db_id = db_id_for_path(path);
    let key = BloomCacheKey { db_id, bucket, last_lsn };
    cg.get(&key)
}

/// Положить биты бакета в кэш (копия bits хранится внутри).
pub fn bloom_cache_put(path: &Path, bucket: u32, last_lsn: u64, bits: Vec<u8>) {
    if let Ok(mut cg) = cache_lock().lock() {
        cg.init_if_needed();
        if !cg.enabled() {
            return;
        }
        let db_id = db_id_for_path(path);
        let key = BloomCacheKey { db_id, bucket, last_lsn };
        cg.put(key, bits);
    }
}

/// Статистика кэша (capacity, entries).
pub fn bloom_cache_stats() -> (usize, usize) {
    if let Ok(mut cg) = cache_lock().lock() {
        return cg.stats();
    }
    (0, 0)
}

/// Счётчики попаданий/промахов.
pub fn bloom_cache_counters() -> (u64, u64) {
    if let Ok(mut cg) = cache_lock().lock() {
        return cg.counters();
    }
    (0, 0)
}