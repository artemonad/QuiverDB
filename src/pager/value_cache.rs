//! pager/value_cache — глобальный LRU‑кэш распакованных OVERFLOW‑значений.
//!
//! Назначение
//! - Ускорить повторные чтения больших значений, которые лежат в OVERFLOW3‑цепочке,
//!   за счёт кэширования уже собранного вектора байт.
//!
//! Ключ
//! - (db_id u64, head_pid u64, total_len u64).
//!
//! Управление и ENV
//! - P1_VALUE_CACHE_BYTES — общий лимит по байтам (default 0 = выключено).
//! - P1_VALUE_CACHE_MIN_SIZE — минимальный размер значения для кэширования (default 4096).
//!
//! API
//! - value_cache_configure(cap_bytes, min_size)
//! - value_cache_clear()
//! - value_cache_get(db_id, head_pid, total_len) -> Option<Vec<u8>>
//! - value_cache_put(db_id, head_pid, total_len, bytes)
//! - value_cache_stats() -> (cap_bytes, used_bytes, entries)
//! - value_cache_counters() -> (hits, misses)
//!
//! Примечания
//! - Храним копии значений (Vec<u8>) — не отдаём ссылки на внутреннее хранилище.
//! - Вставка/обновление учитывает лимит по байтам: вытесняем LRU‑элементы, пока не поместимся.
//! - Если значение меньше min_size или больше cap_bytes — не кэшируем.
//!
//! Интеграция
//! - Использовать в местах раскрытия OVERFLOW: вместо прямого чтения цепочки сначала
//!   value_cache_get(...), а после успешного чтения — value_cache_put(...).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct VCKey {
    db_id: u64,
    head_pid: u64,
    total_len: u64,
}

#[derive(Clone)]
struct Node {
    value: Vec<u8>,
    prev: Option<VCKey>,
    next: Option<VCKey>,
    size: usize,
}

struct ValueCache {
    // Настройки
    cap_bytes: usize,
    min_size: usize,
    inited: bool,
    configured: bool,

    // Данные
    map: HashMap<VCKey, Node>,
    head: Option<VCKey>, // LRU
    tail: Option<VCKey>, // MRU
    used_bytes: usize,

    // Счётчики
    hits: u64,
    misses: u64,
}

impl ValueCache {
    fn new() -> Self {
        Self {
            cap_bytes: 0,
            min_size: 4096,
            inited: false,
            configured: false,
            map: HashMap::new(),
            head: None,
            tail: None,
            used_bytes: 0,
            hits: 0,
            misses: 0,
        }
    }

    fn init_if_needed(&mut self) {
        if self.inited {
            return;
        }
        if !self.configured {
            let cap = std::env::var("P1_VALUE_CACHE_BYTES")
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok())
                .unwrap_or(0);
            let min = std::env::var("P1_VALUE_CACHE_MIN_SIZE")
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok())
                .unwrap_or(4096);
            self.cap_bytes = cap;
            self.min_size = min;
        }
        self.inited = true;
    }

    #[inline]
    fn enabled(&self) -> bool {
        self.cap_bytes > 0
    }

    fn clear(&mut self) {
        self.map.clear();
        self.head = None;
        self.tail = None;
        self.used_bytes = 0;
        self.hits = 0;
        self.misses = 0;
    }

    fn configure(&mut self, cap_bytes: usize, min_size: usize) {
        self.cap_bytes = cap_bytes;
        self.min_size = min_size;
        self.configured = true;
        self.inited = true;
        self.clear();
    }

    fn get(&mut self, key: &VCKey) -> Option<Vec<u8>> {
        if !self.enabled() {
            return None;
        }

        // Сначала локально склонируем значение под короткой мутабельной сессией,
        // затем вне области вызовем move_to_back (иначе двойное &mut self).
        let got: Option<Vec<u8>> = {
            if let Some(n) = self.map.get_mut(key) {
                self.hits = self.hits.saturating_add(1);
                Some(n.value.clone())
            } else {
                None
            }
        };

        match got {
            Some(v) => {
                // теперь переместим в MRU (borrow на map уже сброшен)
                self.move_to_back(key);
                Some(v)
            }
            None => {
                self.misses = self.misses.saturating_add(1);
                None
            }
        }
    }

    fn put(&mut self, key: VCKey, bytes: &[u8]) {
        if !self.enabled() {
            return;
        }
        let n = bytes.len();
        if n < self.min_size || n > self.cap_bytes {
            return;
        }

        // Обновление существующего узла: двуслойно, чтобы избежать конфликтов заимствований.
        if let Some(old_size) = self.map.get(&key).map(|node| node.size) {
            // 1) Сначала перенесём ключ в MRU, чтобы ensure_space не мог его вытеснить.
            self.move_to_back(&key);

            // 2) Если расширяем — освободим место до обновления entry.
            if n > old_size {
                let delta = n - old_size;
                self.ensure_space(delta);
                self.used_bytes = self.used_bytes.saturating_add(delta);
            } else if old_size > n {
                let delta = old_size - n;
                self.used_bytes = self.used_bytes.saturating_sub(delta);
            }

            // 3) Теперь обновим буфер внутри entry в отдельной краткой сессии get_mut.
            if let Some(node) = self.map.get_mut(&key) {
                node.value.resize(n, 0);
                node.value[..].copy_from_slice(bytes);
                node.size = n;
            } else {
                // крайне маловероятно (если ключ исчез), но на всякий случай — fallback в вставку
                self.insert_fresh(key, bytes);
            }
            return;
        }

        // Вставка нового узла
        self.insert_fresh(key, bytes);
    }

    fn insert_fresh(&mut self, key: VCKey, bytes: &[u8]) {
        let n = bytes.len();
        if !self.enabled() || n < self.min_size || n > self.cap_bytes {
            return;
        }
        self.ensure_space(n);
        if n > self.cap_bytes.saturating_sub(self.used_bytes) {
            // всё ещё нет места — не кэшируем
            return;
        }
        let node = Node {
            value: bytes.to_vec(),
            prev: None,
            next: None,
            size: n,
        };
        self.map.insert(key, node);
        self.push_back(key);
        self.used_bytes = self.used_bytes.saturating_add(n);
    }

    fn ensure_space(&mut self, need_more: usize) {
        if !self.enabled() || need_more == 0 {
            return;
        }
        while self.used_bytes.saturating_add(need_more) > self.cap_bytes {
            if !self.evict_one() {
                break;
            }
        }
    }

    fn evict_one(&mut self) -> bool {
        let Some(k) = self.head else {
            return false;
        };
        // Считаем next до мутабельных операций.
        let next = self.map.get(&k).and_then(|n| n.next);

        if let Some(nk) = next {
            if let Some(nnode) = self.map.get_mut(&nk) {
                nnode.prev = None;
            }
            self.head = Some(nk);
        } else {
            self.head = None;
            self.tail = None;
        }
        if let Some(old) = self.map.remove(&k) {
            self.used_bytes = self.used_bytes.saturating_sub(old.size);
        }
        true
    }

    fn push_back(&mut self, key: VCKey) {
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

    fn move_to_back(&mut self, key: &VCKey) {
        if self.tail.as_ref() == Some(key) {
            return;
        }
        // Считаем prev/next, не держа &mut на map
        let (prev, next) = match self.map.get(key) {
            Some(n) => (n.prev, n.next),
            None => return,
        };

        // Отцепим из списка
        match prev {
            Some(pk) => {
                if let Some(pn) = self.map.get_mut(&pk) {
                    pn.next = next;
                }
            }
            None => {
                // key был head
                self.head = next;
            }
        }
        match next {
            Some(nk) => {
                if let Some(nn) = self.map.get_mut(&nk) {
                    nn.prev = prev;
                }
            }
            None => {
                // key был tail — уже обработано выше
            }
        }

        // Вставим в MRU (в хвост)
        match self.tail {
            Some(tk) => {
                if let Some(tail_node) = self.map.get_mut(&tk) {
                    tail_node.next = Some(*key);
                }
                if let Some(me) = self.map.get_mut(key) {
                    me.prev = Some(tk);
                    me.next = None;
                }
                self.tail = Some(*key);
            }
            None => {
                // список пуст — ключ становится и head, и tail
                if let Some(me) = self.map.get_mut(key) {
                    me.prev = None;
                    me.next = None;
                }
                self.head = Some(*key);
                self.tail = Some(*key);
            }
        }
    }

    fn stats(&mut self) -> (usize, usize, usize) {
        self.init_if_needed();
        (self.cap_bytes, self.used_bytes, self.map.len())
    }

    fn counters(&mut self) -> (u64, u64) {
        self.init_if_needed();
        (self.hits, self.misses)
    }
}

static VALUE_CACHE: OnceLock<Mutex<ValueCache>> = OnceLock::new();

#[inline]
fn cache_lock() -> &'static Mutex<ValueCache> {
    VALUE_CACHE.get_or_init(|| Mutex::new(ValueCache::new()))
}

/// Programmatic configuration: set byte capacity and minimum value size.
pub fn value_cache_configure(cap_bytes: usize, min_size: usize) {
    if let Ok(mut cg) = cache_lock().lock() {
        cg.configure(cap_bytes, min_size);
    }
}

/// Clear cache contents (keeps current capacity/min_size settings).
pub fn value_cache_clear() {
    if let Ok(mut cg) = cache_lock().lock() {
        cg.clear();
    }
}

/// Try get cached value (copy).
pub fn value_cache_get(db_id: u64, head_pid: u64, total_len: usize) -> Option<Vec<u8>> {
    let mut cg = cache_lock().lock().ok()?;
    cg.init_if_needed();
    if !cg.enabled() {
        return None;
    }
    let key = VCKey {
        db_id,
        head_pid,
        total_len: total_len as u64,
    };
    cg.get(&key)
}

/// Put value into cache (copy).
pub fn value_cache_put(db_id: u64, head_pid: u64, total_len: usize, bytes: &[u8]) {
    if let Ok(mut cg) = cache_lock().lock() {
        cg.init_if_needed();
        if !cg.enabled() {
            return;
        }
        let key = VCKey {
            db_id,
            head_pid,
            total_len: total_len as u64,
        };
        cg.put(key, bytes);
    }
}

/// Stats: (cap_bytes, used_bytes, entries)
pub fn value_cache_stats() -> (usize, usize, usize) {
    if let Ok(mut cg) = cache_lock().lock() {
        return cg.stats();
    }
    (0, 0, 0)
}

/// Counters: (hits, misses)
pub fn value_cache_counters() -> (u64, u64) {
    if let Ok(mut cg) = cache_lock().lock() {
        return cg.counters();
    }
    (0, 0)
}
