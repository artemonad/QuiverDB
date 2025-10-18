//! page/common — общие константы/offset’ы для v3 страниц (KV_RH3, OVERFLOW3) и трейлера checksum.

// ---------- Общий префикс и типы ----------

/// 4-байтовая магия страницы v3.
pub const PAGE_MAGIC: &[u8; 4] = b"P2PG";

/// Версия формата страниц.
pub const PAGE_VERSION_V3: u16 = 3;

/// Тип страницы: KV (Robin Hood v3)
pub const PAGE_TYPE_KV_RH3: u16 = 2;

/// Тип страницы: OVERFLOW3 (чейн для больших значений)
pub const PAGE_TYPE_OVERFLOW3: u16 = 3;

/// Фиксированная длина трейлера checksum.
pub const TRAILER_LEN: usize = 16;

// ---------- Общие offsets (общий префикс заголовка) ----------
/// Смещение MAGIC (4 байта).
pub const OFF_MAGIC: usize = 0;
/// Смещение version (u16).
pub const OFF_VERSION: usize = 4;
/// Смещение type (u16).
pub const OFF_TYPE: usize = 6;
/// Смещение page_id (u64).
pub const OFF_PAGE_ID: usize = 8;

// ---------- KV_RH3 (type=2) ----------
/// Минимальный размер заголовка KV (с учётом общего префикса).
pub const KV_HDR_MIN: usize = 64;

/// data_start (u32): первый свободный байт в data‑области.
pub const KV_OFF_DATA_START: usize = 16;
/// table_slots (u32): ёмкость таблицы слотов.
pub const KV_OFF_TABLE_SLOTS: usize = 20;
/// used_slots (u32): занятые слоты.
pub const KV_OFF_USED_SLOTS: usize = 24;
/// flags (u32): флаги (резерв).
pub const KV_OFF_FLAGS: usize = 28;
/// next_page_id (u64): ссылка на следующую страницу цепочки.
pub const KV_OFF_NEXT_PID: usize = 32;
/// lsn (u64): LSN записи страницы.
pub const KV_OFF_LSN: usize = 40;
/// codec_id (u16): 0=none, 1=zstd, 2=lz4 (резерв).
pub const KV_OFF_CODEC_ID: usize = 48;

/// Размер слота Robin Hood (для будущего многозаписочного режима): [off u32][fp u8][dist u8].
pub const KV_SLOT_SIZE: usize = 6;
/// Пустой off в слоте.
pub const KV_EMPTY_OFF: u32 = u32::MAX;

// ---------- OVERFLOW3 (type=3) ----------
/// Минимальный размер заголовка OVERFLOW3 (с учётом общего префикса).
pub const OVF_HDR_MIN: usize = 64;

/// chunk_len (u32): байт полезной нагрузки на странице (сжатая длина, если codec!=0).
pub const OVF_OFF_CHUNK_LEN: usize = 16;
/// reserved (u32).
pub const OVF_OFF_RESERVED: usize = 20;
/// next_page_id (u64): ссылка на следующую страницу цепочки или u64::MAX (NO_PAGE).
pub const OVF_OFF_NEXT_PID: usize = 24;
/// lsn (u64): LSN страницы overflow.
pub const OVF_OFF_LSN: usize = 32;
/// codec_id (u16): 0=none, 1=zstd, 2=lz4 (резерв).
pub const OVF_OFF_CODEC_ID: usize = 40;
