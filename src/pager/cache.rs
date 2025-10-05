use std::collections::HashMap;

/// Простой LRU‑подобный кэш страниц по количеству записей.
/// Эвикт — по минимальному last_access (O(n)), при капе ~128/256 это нормально.
pub(crate) struct PageCache {
    cap: usize,
    page_size: usize,
    map: HashMap<u64, CacheEntry>,
    tick: u64,
}

struct CacheEntry {
    data: Vec<u8>,
    last_access: u64,
}

impl PageCache {
    /// Создать кэш на N страниц заданного размера.
    pub(crate) fn new(cap: usize, page_size: usize) -> Self {
        Self {
            cap,
            page_size,
            map: HashMap::with_capacity(cap),
            tick: 0,
        }
    }

    /// Прочитать страницу из кэша в `out`, обновив last_access.
    /// Возвращает true, если страница найдена и скопирована.
    pub(crate) fn get_mut(&mut self, page_id: u64, out: &mut [u8]) -> bool {
        if let Some(e) = self.map.get_mut(&page_id) {
            if e.data.len() == out.len() {
                out.copy_from_slice(&e.data);
                self.tick = self.tick.wrapping_add(1);
                e.last_access = self.tick;
                return true;
            }
        }
        false
    }

    /// Положить страницу в кэш (копирует данные).
    pub(crate) fn put(&mut self, page_id: u64, data: &[u8]) {
        if data.len() != self.page_size {
            return;
        }
        self.tick = self.tick.wrapping_add(1);
        let entry = CacheEntry {
            data: data.to_vec(),
            last_access: self.tick,
        };

        // Если новый ключ и кэш полон — эвиктим наименее недавно использованный
        if self.map.len() >= self.cap && !self.map.contains_key(&page_id) {
            let mut victim: Option<(u64, u64)> = None; // (pid, last_access)
            for (pid, e) in self.map.iter() {
                let la = e.last_access;
                victim = match victim {
                    None => Some((*pid, la)),
                    Some((_, best)) if la < best => Some((*pid, la)),
                    v => v,
                };
            }
            if let Some((victim_pid, _)) = victim {
                self.map.remove(&victim_pid);
            }
        }

        self.map.insert(page_id, entry);
    }
}