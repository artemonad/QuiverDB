#![allow(non_snake_case)]

// Базовые модули
pub mod config;
pub mod dir;
pub mod meta;
pub mod metrics;

// Новая модульная раскладка (папки с mod.rs)
pub mod db; // src/db/{mod,core,open,kv,batch,scan,maintenance,doctor,compaction,vacuum}.rs
pub mod free;
pub mod page; // src/page/{mod,common,checksum,kv,ovf}.rs
pub mod pager; // src/pager/{mod,core,io,alloc,commit,replay}.rs
pub mod wal; // src/wal/{mod,writer,replay}.rs // src/free/mod.rs

// Криптография (TDE prep)
pub mod crypto; // src/crypto/mod.rs

// Bloom side-car (per-bucket hints)
pub mod bloom; // src/bloom/mod.rs

// Утилиты (now_secs, decode_ovf_placeholder_v3, ...)
pub mod util; // src/util/mod.rs

// NEW: SnapStore v2 (контент-адресное хранилище с refcount)
pub mod snapstore; // src/snapstore/mod.rs

// NEW: FFI (C ABI) — включается фичей "ffi"
#[cfg(feature = "ffi")]
pub mod ffi;

// Удобные реэкспорты
pub use db::Db;
pub use dir::Directory;
pub use meta::{
    read_meta, set_clean_shutdown, set_last_lsn, validate_page_size, write_meta_new,
    write_meta_overwrite, MetaHeader,
};

// Реэкспорты crypto API (для удобства использования из внешнего кода)
pub use crypto::{derive_gcm_nonce, EnvKeyProvider, KeyMaterial, KeyProvider, StaticKeyProvider};
