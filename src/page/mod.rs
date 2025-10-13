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

pub mod common;
pub mod checksum;
pub mod kv;
pub mod ovf;
// NEW: публичный модуль для packing
pub mod kv_pack;

// ---------------- re-exports (внешний API модуля page) ----------------

pub use common::{
    PAGE_MAGIC, PAGE_VERSION_V3,
    PAGE_TYPE_KV_RH3, PAGE_TYPE_OVERFLOW3,
    TRAILER_LEN,
    // KV header layout essentials
    KV_HDR_MIN,
    // OVF header layout essentials
    OVF_HDR_MIN,
    // offsets used by pager/commit/replay
    OFF_TYPE, KV_OFF_LSN, OVF_OFF_LSN,
};

pub use checksum::{
    // 2.0 CRC32C
    page_update_checksum, page_verify_checksum,
    // 2.1 prep (TDE, AES-GCM tag-only)
    page_update_trailer_aead_with, page_verify_trailer_aead_with,
    // Helpers (CRC trailer field access / zero-check)
    page_trailer_crc32_le, page_trailer_is_zero_crc32,
};

pub use kv::{
    KvHeaderV3,
    kv_init_v3,
    kv_header_read_v3,
    kv_header_write_v3,
    kv_read_record,
};

pub use ovf::{
    OvfHeaderV3,
    ovf_init_v3,
    ovf_header_read_v3,
    ovf_header_write_v3,
};