//! db — high-level API слоя базы данных (v2.0+)
//!
//! Разделение по подмодулям:
//! - core.rs        — базовые типы (Db), поля, константы, lock-хэндлинг, init()
//! - open.rs        — открытие/закрытие (open/open_ro + _with_config), привязка QuiverConfig
//! - kv.rs          — одиночные операции (put/get/del), TTL/tombstone семантика
//! - exists.rs      — быстрый presence‑check (keydir/bloom fast‑path)
//! - batch.rs       — Batch API (KV‑packing, OVERFLOW, единый WAL‑батч HEADS_UPDATE)
//! - scan.rs        — сканы (keydir fast‑path и chain‑path)
//! - maintenance.rs — обслуживание: sweep orphan overflow, print_stats, doctor-сканер
//! - doctor.rs      — doctor-скан (CRC/IO) с JSON-отчётом
//! - compaction.rs  — онлайн-компактация цепочек (bucket/all)
//! - vacuum.rs      — вакуум: compaction_all + sweep_orphan_overflow

pub mod core;
pub mod open;
pub mod kv;
pub mod exists;
pub mod batch;
pub mod scan;
pub mod maintenance;
pub mod doctor;
pub mod compaction;
pub mod vacuum;

pub use core::Db;