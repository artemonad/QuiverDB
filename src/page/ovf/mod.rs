//! page/ovf — декомпозированный модуль OVERFLOW3 (v3).
//! - header.rs — заголовок OVF страницы (init/read/write)
//! - chain.rs  — helpers для чтения OVERFLOW‑цепочек (codec-aware)

pub mod header;
pub mod chain;

// Реэкспорт внешнего API (имена сохраняем)
pub use header::{OvfHeaderV3, ovf_init_v3, ovf_header_read_v3, ovf_header_write_v3};