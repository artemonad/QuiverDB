//! pager/alloc — аллокация страниц и подготовка сегментов.
//!
//! Обновления:
//! - allocate_one_page использует free‑лист (см. ранее) только если файл существует (без лишнего open()).
//! - allocate_pages больше НЕ делает перезапись meta на каждую аллокацию (write_meta_overwrite).
//!   self.meta.next_page_id обновляется только в памяти; запись meta происходит при Drop(Db)
//!   (clean_shutdown=true) или вручную через CLI checkpoint.
//! - allocate_pages по‑прежнему не делает fsync на расширение сегмента (set_len).
//! - NEW: hot preallocation — заранее расширяем последний затронутый сегмент на N страниц
//!   (ENV P1_PREALLOC_PAGES=N), чтобы уменьшить число set_len при серии аллокаций.
//!   Предаллокация ограничена границей сегмента (SEGMENT_SIZE) и НЕ меняет next_page_id.
//!
//! Примечание: предаллокация изменяет только длину файла сегмента, но не логическое число
//! выделенных страниц (next_page_id). Это прозрачная оптимизация I/O.

use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::free::FreeList;
// write_meta_overwrite удалён из горячего пути
// use crate::meta::write_meta_overwrite;

use super::core::Pager;

impl Pager {
    /// Аллокация последовательности новых страниц. Возвращает начальный page_id.
    ///
    /// Правила:
    /// - Вычисляет, какие сегменты и до какой длины нужно увеличить, чтобы поместить все новые страницы.
    /// - Для каждого затронутого сегмента делает set_len(need_len).
    ///   ВНИМАНИЕ: fsync на расширение НЕ выполняется (коалесцировано в commit_pages_batch).
    /// - Обновляет meta.next_page_id = end ТОЛЬКО в памяти (без write_meta_overwrite).
    /// - NEW: на последний затронутый сегмент может быть добавлена предаллокация (см. P1_PREALLOC_PAGES).
    ///
    /// Примечание:
    /// - Для последовательностей (>1) используем только “свежие” page_id (континуум),
    ///   чтобы сохранить требование непрерывности (нужно для OVERFLOW3 цепочек).
    pub fn allocate_pages(&mut self, count: u64) -> Result<u64> {
        let start = self.meta.next_page_id;
        let end = start + count;

        // Находим максимальную требуемую длину для каждого сегмента.
        let mut seg_max_len: BTreeMap<u64, u64> = BTreeMap::new();
        for pid in start..end {
            let (seg_no, off) = self.locate(pid);
            let need = off + (self.meta.page_size as u64);
            seg_max_len
                .entry(seg_no)
                .and_modify(|v| *v = (*v).max(need))
                .or_insert(need);
        }

        // NEW: hot preallocation для последнего затронутого сегмента (если включено ENV).
        if let Some((&last_seg_no, &need_len0)) = seg_max_len.iter().next_back() {
            let pre_pages = prealloc_pages();
            if pre_pages > 0 {
                warn_prealloc_once(pre_pages);
                let ps_u64 = self.meta.page_size as u64;
                let seg_cap_len = self.pages_per_seg() * ps_u64;
                let slack_bytes = pre_pages.saturating_mul(ps_u64);
                let mut new_need = need_len0.saturating_add(slack_bytes);
                if new_need > seg_cap_len {
                    new_need = seg_cap_len;
                }
                if new_need > need_len0 {
                    // Обновим карту с новым целевым размером для последнего сегмента
                    seg_max_len.insert(last_seg_no, new_need);
                }
            }
        }

        // Увеличиваем длины сегментов до нужной (без fsync на расширение).
        for (seg_no, need_len) in seg_max_len {
            let f = self.open_seg_rw(seg_no, true)?;
            let cur_len = f.metadata()?.len();
            if cur_len < need_len {
                // Дорастим файл до нужного размера; fsync сделает commit_pages_batch (коалесцировано).
                f.set_len(need_len)?;
            }
        }

        // Продвигаем next_page_id (в памяти; без write_meta_overwrite).
        self.meta.next_page_id = end;
        Ok(start)
    }

    /// Аллокация одной страницы.
    ///
    /// Попытка 1: взять page_id из free‑листа (<root>/free) — ТОЛЬКО если файл существует.
    /// Попытка 2: fallback на allocate_pages(1) — выделить новый page_id из хвоста.
    pub fn allocate_one_page(&mut self) -> Result<u64> {
        // Проверим, существует ли free‑лист; избегаем лишнего open() с ошибкой на каждый вызов.
        let free_path = self.root.join("free");
        if free_path.exists() {
            if let Ok(fl) = FreeList::open(&self.root) {
                if let Ok(Some(pid)) = fl.pop() {
                    // Best‑effort: гарантировать, что сегмент достаточно длинный
                    let _ = self.ensure_allocated(pid);
                    return Ok(pid);
                }
            }
        }
        // Free‑лист пуст/недоступен — выделим новую
        self.allocate_pages(1)
    }
}

// ---------- ENV helpers (cached) ----------

fn prealloc_pages() -> u64 {
    static PRE: OnceLock<u64> = OnceLock::new();
    *PRE.get_or_init(|| {
        std::env::var("P1_PREALLOC_PAGES")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0)
    })
}

fn warn_prealloc_once(pages: u64) {
    static WARNED: OnceLock<()> = OnceLock::new();
    if pages == 0 {
        return;
    }
    WARNED.get_or_init(|| {
        eprintln!(
            "[INFO] Hot preallocation enabled: P1_PREALLOC_PAGES={} (pages per last touched segment). \
             Preallocation is bounded by segment size and does not change next_page_id.",
            pages
        );
    });
}
