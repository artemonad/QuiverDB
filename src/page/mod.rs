//! page — v3 on-disk page types (KV_RH3, OVERFLOW3) + fixed checksum trailer.
//!
//! Layout/format details — см. docs/format.md.
//!
//! Разделение по подмодулям:
//! - common.rs   — общие константы/offset’ы, MAGIC/версия/типы, размеры заголовков.
//! - checksum.rs — трейлер: CRC32C (2.0) и AEAD-tag (AES-GCM, 2.1 prep) + хелперы трейлера.
//! - kv.rs       — KV_RH3: init/read/write заголовка и helper для чтения записи.
//! - ovf.rs      — OVERFLOW3: init/read/write заголовка.
//! - kv_pack.rs  — упаковка нескольких KV-записей на одну страницу (поддержка packing).

pub mod checksum;
pub mod common;
pub mod kv;
pub mod ovf;
// NEW: публичный модуль для packing
pub mod kv_pack;

// ---------------- re-exports (внешний API модуля page) ----------------

pub use common::{
    // KV header layout essentials
    KV_HDR_MIN,
    KV_OFF_LSN,
    // offsets used by pager/commit/replay
    OFF_TYPE,
    // OVF header layout essentials
    OVF_HDR_MIN,
    OVF_OFF_LSN,
    PAGE_MAGIC,
    PAGE_TYPE_KV_RH3,
    PAGE_TYPE_OVERFLOW3,
    PAGE_VERSION_V3,
    TRAILER_LEN,
};

pub use checksum::{
    // Helpers (CRC trailer field access / zero-check)
    page_trailer_crc32_le,
    page_trailer_is_zero_crc32,
    // 2.0 CRC32C
    page_update_checksum,
    // 2.1 prep (TDE, AES-GCM tag-only)
    page_update_trailer_aead_with,
    page_verify_checksum,
    page_verify_trailer_aead_with,
    // NEW: строгая CRC‑проверка (единая точка правды)
    verify_page_crc_strict_kind,
};

pub use kv::{
    kv_header_read_v3,
    kv_header_write_v3,
    // Устаревший kv_read_record больше не реэкспортируем, используйте безопасные хелперы из kv::
    // kv_read_record_at_checked, kv_for_each_record, kv_for_each_record_with_off, kv_find_record_by_key, ...
    kv_init_v3,
    KvHeaderV3,
};

pub use ovf::{ovf_header_read_v3, ovf_header_write_v3, ovf_init_v3, OvfHeaderV3};
