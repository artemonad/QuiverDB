//! page/kv — декомпозированный модуль KV_RH3 (v3):
//! - header.rs — заголовок страницы KV (init/read/write)
//! - record.rs — чтение записей, безопасный поиск по слот‑таблице

pub mod header;
pub mod record;

// Реэкспорт внешнего API (имена не меняем)
pub use header::{KvHeaderV3, kv_init_v3, kv_header_read_v3, kv_header_write_v3};
pub use record::{kv_read_record, kv_find_record_by_key, kv_for_each_record};