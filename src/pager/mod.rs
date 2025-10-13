//! pager — слой управления страницами (2.0+), модульная разбивка.
//!
//! Подмодули (реализация добавлена по шагам):
//! - core.rs   — структура Pager, open(), флаги (data_fsync) и общие поля.
//! - io.rs     — ensure_allocated/read/write низкоуровневые операции.
//! - alloc.rs  — аллокация страниц/сегментов.
//! - commit.rs — commit_page/commit_pages_batch (WAL v2 + запись в сегменты).
//! - replay.rs — wal_replay_with_pager обёртка вокруг WAL v2 реплея.
//! - cache.rs  — процессный кэш страниц (second-chance).
//!
//! Публичные константы экспортируются отсюда, чтобы внешний код мог их использовать
//! (например, тесты ссылались на DATA_SEG_PREFIX/EXT).

// Публичные константы сегмента (значения как в 2.0):
/// Размер сегмента данных (байт). По умолчанию 32 MiB.
pub const SEGMENT_SIZE: u64 = 32 * 1024 * 1024;
/// Префикс имени сегмента данных.
pub const DATA_SEG_PREFIX: &str = "data-";
/// Расширение файла сегмента данных (формат 2.0).
pub const DATA_SEG_EXT: &str = "p2seg";

// Подмодули (реализация)
pub mod core;
pub mod io;
pub mod alloc;
pub mod commit;
pub mod replay;
// ВАЖНО: делаем модуль cache публичным, чтобы внешние бинари могли импортировать его API
pub mod cache;

// Re-exports для внешнего API
pub use core::Pager;