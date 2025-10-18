//! page/kv — декомпозированный модуль KV_RH3 (v3):
//! - header.rs — заголовок страницы KV (init/read/write)
//! - record.rs — чтение записей, безопасный поиск по слот‑таблице
//!
//! Внешний API реэкспортируется отсюда.

pub mod header;
pub mod record;

// Реэкспорт внешнего API (имена не меняем, добавлены новые безопасные хелперы)
pub use header::{kv_header_read_v3, kv_header_write_v3, kv_init_v3, KvHeaderV3};

pub use record::{
    // Поиск и обход (packed-aware, reverse слоты “новые→старые”)
    kv_find_record_by_key,
    kv_for_each_record,

    // NEW: обход с оффсетами записей — для keydir (pid, off)
    kv_for_each_record_with_off,
    // NEW: безопасный ридер по известному смещению (с учётом data_end)
    kv_read_record_at_checked,

    // УСТАРЕВШЕ: kv_read_record больше не реэкспортируем. Используйте безопасные хелперы ниже.
    // kv_read_record,

    // Небезопасный ридер без учёта data_end (оставлен для редких случаев/тестов)
    kv_read_record_unchecked,
};
