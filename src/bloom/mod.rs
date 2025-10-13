//! Bloom side-car: разбиение на подмодули
//! - cache.rs    — глобальный LRU‑кэш битов последних N бакетов
//! - sidecar.rs  — реализация BloomSidecar (P2BLM01), чтение/запись/тест

// Делаем модуль cache публичным, чтобы QuiverDB::bloom::cache::* был виден снаружи
pub mod cache;
pub mod sidecar;

// Удобные реэкспорты верхнего уровня
pub use sidecar::BloomSidecar;
pub use cache::{bloom_cache_stats, bloom_cache_counters};