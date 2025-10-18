//! db/read_page — общие хелперы per‑page решений при чтении.
//!
//! Назначение:
//! - Единая логика обхода одной KV‑страницы “новые → старые” с корректной обработкой
//!   tombstone/TTL и поддержкой OVERFLOW placeholder.
//! - Используется get()/exists() для устранения дублирования и багов TTL‑дубликатов.
//!
//! API:
//! - decide_value_on_page(page, key, now) -> DecideOnPage
//! - decide_exists_on_page(page, key, now) -> DecideExists
//!
//! Безопасность:
//! - Если страница не KV_RH3 или MAGIC не совпадает — возвращает Continue.
//! - Обход используется через kv_for_each_record (packed‑aware; reverse слоты).

use crate::metrics::record_ttl_skipped;
use crate::page::kv::kv_for_each_record;
use crate::page::{OFF_TYPE, PAGE_MAGIC, PAGE_TYPE_KV_RH3};
use crate::util::decode_ovf_placeholder_v3;
use byteorder::{ByteOrder, LittleEndian};

/// Решение при чтении value по одной странице.
#[derive(Debug)]
pub enum DecideOnPage {
    Tombstone,
    Valid(Vec<u8>),
    /// На странице встретился OVERFLOW placeholder — нужно раскрыть цепочку на вызывающем уровне.
    NeedOverflow {
        total_len: usize,
        head_pid: u64,
    },
    /// Ничего не нашли или всё протухло — нужно перейти к next_page_id.
    Continue,
}

/// Решение при exists() по одной странице.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecideExists {
    Tombstone,
    Present,
    Continue,
}

/// Проверить страницу и принять решение по ключу для get():
/// - Tombstone → Tombstone
/// - Валидная запись (TTL ок) → Valid(...) или NeedOverflow(...)
/// - Протухшая запись → продолжаем искать более старую версию того же ключа на странице
/// - Иначе → Continue
#[inline]
pub fn decide_value_on_page(page: &[u8], key: &[u8], now: u32) -> DecideOnPage {
    if !is_kv_page(page) {
        return DecideOnPage::Continue;
    }

    let mut decision = DecideOnPage::Continue;

    kv_for_each_record(page, |k, v, expires_at_sec, vflags| {
        if !matches!(decision, DecideOnPage::Continue) {
            return;
        }
        if k != key {
            return;
        }

        // Tombstone — приоритет, немедленное завершение
        if (vflags & 0x1) == 1 {
            decision = DecideOnPage::Tombstone;
            return;
        }

        // TTL read-side
        if expires_at_sec != 0 && now >= expires_at_sec {
            record_ttl_skipped();
            return; // Ищем более старую версию на текущей странице
        }

        // Валидная: inline или placeholder
        if let Some((total_len, head_pid)) = decode_ovf_placeholder_v3(v) {
            decision = DecideOnPage::NeedOverflow {
                total_len: total_len as usize,
                head_pid,
            };
        } else {
            decision = DecideOnPage::Valid(v.to_vec());
        }
    });

    decision
}

/// Проверить страницу и принять решение по ключу для exists():
/// - Tombstone → Tombstone
/// - Валидная запись (TTL ок) → Present
/// - Протухшая запись → продолжаем искать более старую версию того же ключа на странице
/// - Иначе → Continue
#[inline]
pub fn decide_exists_on_page(page: &[u8], key: &[u8], now: u32) -> DecideExists {
    if !is_kv_page(page) {
        return DecideExists::Continue;
    }

    let mut decision = DecideExists::Continue;

    kv_for_each_record(page, |k, _v, expires_at_sec, vflags| {
        if !matches!(decision, DecideExists::Continue) {
            return;
        }
        if k != key {
            return;
        }

        if (vflags & 0x1) == 1 {
            decision = DecideExists::Tombstone;
            return;
        }

        if expires_at_sec != 0 && now >= expires_at_sec {
            record_ttl_skipped();
            return;
        }

        decision = DecideExists::Present;
    });

    decision
}

// ---------- helpers ----------

#[inline]
fn is_kv_page(page: &[u8]) -> bool {
    if page.len() < 12 {
        return false;
    }
    if &page[0..4] != PAGE_MAGIC {
        return false;
    }
    let ptype = LittleEndian::read_u16(&page[OFF_TYPE..OFF_TYPE + 2]);
    ptype == PAGE_TYPE_KV_RH3
}
