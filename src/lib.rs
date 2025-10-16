#![allow(non_snake_case)]

// Базовые модули
pub mod meta;
pub mod dir;
pub mod metrics;
pub mod config;

// Новая модульная раскладка (папки с mod.rs)
pub mod page;   // src/page/{mod,common,checksum,kv,ovf}.rs
pub mod wal;    // src/wal/{mod,writer,replay}.rs
pub mod pager;  // src/pager/{mod,core,io,alloc,commit,replay}.rs
pub mod db;     // src/db/{mod,core,open,kv,batch,scan,maintenance,doctor,compaction,vacuum}.rs
pub mod free;   // src/free/mod.rs

// Криптография (TDE prep)
pub mod crypto; // src/crypto/mod.rs

// Bloom side-car (per-bucket hints)
pub mod bloom;  // src/bloom/mod.rs

// Утилиты (now_secs, decode_ovf_placeholder_v3, ...)
pub mod util;   // src/util/mod.rs

// NEW: FFI (C ABI) — включается фичей "ffi"
#[cfg(feature = "ffi")]
pub mod ffi;

// Удобные реэкспорты
pub use db::Db;
pub use dir::Directory;
pub use meta::{
    read_meta, write_meta_new, write_meta_overwrite,
    set_clean_shutdown, set_last_lsn, validate_page_size, MetaHeader,
};

// Реэкспорты crypto API (для удобства использования из внешнего кода)
pub use crypto::{
    KeyProvider, KeyMaterial, StaticKeyProvider, EnvKeyProvider, derive_gcm_nonce,
};