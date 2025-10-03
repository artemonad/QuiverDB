//! Robin Hood in-page index (v2) — модуль разнесён на подмодули:
//! - header: заголовок страницы, init, CRC, базовые проверки
//! - table: слоты, хэш-таблица, утилиты и rebuild
//! - ops: операции KV (lookup/insert/delete/list/compact), usage/should_compact

mod header;
mod table;
mod ops;

pub use header::{
    RhPageHeader, rh_header_read, rh_header_write, rh_page_init, rh_page_is_kv,
    rh_page_update_crc, rh_page_verify_crc, RH_PAGE_CRC_OFF,
};

pub use table::RH_SLOT_SIZE;

pub use ops::{
    rh_kv_lookup, rh_kv_lookup_slot, rh_kv_insert, rh_kv_delete_inplace, rh_kv_list,
    rh_compact_inplace, rh_page_usage, rh_should_compact,
};