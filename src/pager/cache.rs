//! pager/cache — процессный кэш страниц (second-chance), вынесенный из io.rs.
//!
//! Назначение:
//! - Снизить количество дисковых чтений страниц за счёт простого глобального кэша.
//! - Управляется программно (configure) или через ENV P1_PAGE_CACHE_PAGES (по умолчанию 0 — выключен).
//!
//! ВАЖНО (фикс):
//! - Ключ кэша теперь использует u64 для db_id (раньше был usize, что приводило к усечению на 32‑битных платформах).
//!   Это устраняет риск коллизий между БД при одинаковых page_id.
//!
//! API (процесс-широкий, thread-safe):
//! - page_cache_configure(page_size, cap_pages) — задать размер страницы и ёмкость (страниц). 0 — выключить.
//! - page_cache_clear() — очистить содержимое, сохранив настройки.
//! - page_cache_get(db_id: u64, page_id: u64, page_size) -> Option<Vec<u8>> — получить копию байтов страницы из кэша.
//! - page_cache_put(db_id: u64, page_id: u64, buf) — положить страницу (копия).
//! - page_cache_invalidate(db_id: u64, page_id: u64) — инвалидация конкретного ключа.
//! - page_cache_evictions_total() -> u64 — число выселений (диагностика).
//! - page_cache_len() -> usize — текущее число страниц в кэше.
//!
//! Примечание:
//! - db_id — стабильный идентификатор БД (u64), см. Pager::db_id (u64).
//! - Возвращается Option<Vec<u8>> (копия), чтобы не выдавать ссылку на внутренний буфер за пределы лока.

use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

#[derive(Clone)]
struct CacheEntry {
    buf: Vec<u8>,
    refbit: bool,
}

/// Ключ кэша: (уникальный id БД в процессе, page_id).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct CacheKey {
    db_id: u64,
    page_id: u64,
}

struct GlobalCache {
    // Максимум страниц в кэше
    cap_pages: usize,
    // Размер страницы (в байтах) — для валидации/инициализации
    page_size: usize,
    // Очередь для second-chance (FIFO с refbit)
    q: VecDeque<CacheKey>,
    // Основное хранилище
    map: HashMap<CacheKey, CacheEntry>,
    // Инициализирован ли кэш (ENV/программно)
    inited: bool,
    // Был ли кэш явно сконфигурирован приложением (через API), а не через ENV
    configured: bool,
    // Счётчик выселений (диагностика)
    evictions_total: u64,
}

impl GlobalCache {
    fn new() -> Self {
        Self {
            cap_pages: 0,
            page_size: 0,
            q: VecDeque::new(),
            map: HashMap::new(),
            inited: false,
            configured: false,
            evictions_total: 0,
        }
    }

    fn init_if_needed(&mut self, page_size: usize) {
        if self.inited {
            if self.page_size != page_size && self.enabled() {
                // Смена геометрии — сбросим содержимое, сохранив активную конфигурацию.
                self.q.clear();
                self.map.clear();
                self.page_size = page_size;
                self.evictions_total = 0;
            } else if self.page_size == 0 {
                self.page_size = page_size;
            }
            return;
        }
        if !self.configured {
            // Lazy ENV init. По умолчанию (без ENV) кэш выключен (0 страниц).
            let cap = std::env::var("P1_PAGE_CACHE_PAGES")
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok())
                .unwrap_or(0);
            self.cap_pages = cap;
        }
        self.page_size = page_size;
        self.inited = true;
    }

    #[inline]
    fn enabled(&self) -> bool {
        self.cap_pages > 0
    }

    #[inline]
    fn len(&self) -> usize {
        self.map.len()
    }

    fn get(&mut self, key: &CacheKey) -> Option<Vec<u8>> {
        if !self.enabled() {
            return None;
        }
        if let Some(ent) = self.map.get_mut(key) {
            ent.refbit = true;
            return Some(ent.buf.clone());
        }
        None
    }

    fn put(&mut self, key: CacheKey, src: &[u8]) {
        if !self.enabled() {
            return;
        }
        // Вставка/обновление: если ключ уже есть — обновим буфер и refbit.
        if let Some(ent) = self.map.get_mut(&key) {
            ent.buf.resize(src.len(), 0);
            ent.buf.copy_from_slice(src);
            ent.refbit = true;
            return;
        }

        // second-chance eviction до помещения нового элемента
        while self.map.len() >= self.cap_pages {
            if !self.evict_one() {
                // если очередь пуста (патологический случай рассогласования) — прервём цикл
                break;
            }
        }

        // Вставка нового элемента
        let mut buf = vec![0u8; src.len()];
        buf.copy_from_slice(src);
        self.q.push_back(key);
        self.map.insert(
            key,
            CacheEntry {
                buf,
                refbit: true,
            },
        );
    }

    fn evict_one(&mut self) -> bool {
        // second-chance: ищем ключ без refbit; если refbit=1 — обнуляем и отправляем в хвост.
        while let Some(k) = self.q.pop_front() {
            if let Some(e) = self.map.get_mut(&k) {
                if e.refbit {
                    e.refbit = false;
                    self.q.push_back(k);
                    continue;
                }
            }
            // либо нет записи (ленивое очищение), либо refbit=0 — можно выселять
            let _ = self.map.remove(&k);
            self.evictions_total = self.evictions_total.saturating_add(1);
            return true;
        }
        false
    }

    fn invalidate(&mut self, key: &CacheKey) {
        if !self.enabled() {
            return;
        }
        let present = self.map.remove(key).is_some();
        if present {
            // Очередь чистим лениво (remove из map достаточно для корректности second-chance)
            self.evictions_total = self.evictions_total.saturating_add(1);
        }
    }

    fn configure(&mut self, page_size: usize, cap_pages: usize) {
        // Меняем настройки: при смене page_size сбросим содержимое.
        if self.page_size != 0 && self.page_size != page_size {
            self.q.clear();
            self.map.clear();
            self.evictions_total = 0;
        }
        self.page_size = page_size;
        self.cap_pages = cap_pages;
        self.configured = true;
        self.inited = true;

        if self.cap_pages == 0 {
            self.q.clear();
            self.map.clear();
            self.evictions_total = 0;
        } else {
            // Если новая ёмкость меньше текущей — принудительно выселим лишние.
            while self.map.len() > self.cap_pages {
                if !self.evict_one() {
                    break;
                }
            }
        }
    }

    fn clear(&mut self) {
        self.q.clear();
        self.map.clear();
        self.evictions_total = 0;
    }
}

static GLOBAL_CACHE: OnceLock<Mutex<GlobalCache>> = OnceLock::new();

#[inline]
fn cache_lock() -> &'static Mutex<GlobalCache> {
    GLOBAL_CACHE.get_or_init(|| Mutex::new(GlobalCache::new()))
}

/// Programmatic configuration of the global page cache (process-wide).
/// Safe to call multiple times; switching page_size resets cache contents.
/// Setting cap_pages=0 disables the cache.
pub fn page_cache_configure(page_size: usize, cap_pages: usize) {
    let mut cg = cache_lock().lock().unwrap();
    cg.configure(page_size, cap_pages);
}

/// Clears cache contents (keeps current capacity/page_size settings).
pub fn page_cache_clear() {
    let mut cg = cache_lock().lock().unwrap();
    cg.clear();
}

/// Ensure the cache is initialized (ENV or configured) for given page_size.
pub fn page_cache_init(page_size: usize) {
    let mut cg = cache_lock().lock().unwrap();
    cg.init_if_needed(page_size);
}

/// Try get page bytes from cache (copy). Returns None on miss or disabled cache.
pub fn page_cache_get(db_id: u64, page_id: u64, page_size: usize) -> Option<Vec<u8>> {
    let mut cg = cache_lock().lock().ok()?;
    cg.init_if_needed(page_size);
    cg.get(&CacheKey { db_id, page_id })
}

/// Put page bytes into cache (copy).
pub fn page_cache_put(db_id: u64, page_id: u64, buf: &[u8], page_size: usize) {
    if let Ok(mut cg) = cache_lock().lock() {
        cg.init_if_needed(page_size);
        cg.put(CacheKey { db_id, page_id }, buf);
    }
}

/// Invalidate a cached page entry.
pub fn page_cache_invalidate(db_id: u64, page_id: u64, page_size: usize) {
    if let Ok(mut cg) = cache_lock().lock() {
        cg.init_if_needed(page_size);
        cg.invalidate(&CacheKey { db_id, page_id });
    }
}

/// Diagnostics: total number of evictions since start (or last reconfigure/clear).
pub fn page_cache_evictions_total() -> u64 {
    if let Ok(cg) = cache_lock().lock() {
        return cg.evictions_total;
    }
    0
}

/// Diagnostics: current number of pages in the cache.
pub fn page_cache_len() -> usize {
    if let Ok(cg) = cache_lock().lock() {
        return cg.len();
    }
    0
}