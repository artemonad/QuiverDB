//! Общие константы форматов (meta, data segments, pages, directory, WAL).

// -------- Meta --------
pub const MAGIC: &[u8; 8] = b"P1DBMETA";
pub const META_FILE: &str = "meta";

// -------- Data segments --------
pub const DATA_SEG_PREFIX: &str = "data-";
pub const DATA_SEG_EXT: &str = "p1seg";
pub const SEGMENT_SIZE: u64 = 32 * 1024 * 1024;

// -------- Flags (reserved for future features) --------
pub const FLAG_TDE_ENABLED: u32 = 0x1;

// -------- Pages (v2: Robin Hood + Overflow) --------
pub const PAGE_MAGIC: &[u8; 4] = b"P1PG";
pub const PAGE_TYPE_KV_RH: u16 = 2;
// Новый тип страницы под overflow-данные (v2-страница, общий 64-байтовый заголовок).
pub const PAGE_TYPE_OVERFLOW: u16 = 3;
pub const PAGE_HDR_V2_SIZE: usize = 64; // [MAGIC4][ver u16=2][type u16][pad4][page_id u64][data_start u16][table_slots u16][used_slots u16][flags u16][next_page_id u64][lsn u64][seed u64][crc32 u32]

pub const NO_PAGE: u64 = u64::MAX;

// -------- Directory --------
pub const DIR_FILE: &str = "dir";
pub const DIR_MAGIC: &[u8; 8] = b"P1DIR001";
pub const DIR_HDR_SIZE: usize = 24; // [magic8][ver u32][buckets u32][reserved u64]

// -------- Free-list (v0.6) --------
// Отдельный файл со списком свободных страниц.
// Формат header (24 байта):
// [magic8="P1FREE01"][ver u32=1][count u32][reserved u64]
// Далее последовательность u64 page_id (count штук).
pub const FREE_FILE: &str = "free";
pub const FREE_MAGIC: &[u8; 8] = b"P1FREE01";
pub const FREE_HDR_SIZE: usize = 24;

// -------- WAL --------
pub const WAL_FILE: &str = "wal-000001.log";
pub const WAL_MAGIC: &[u8; 8] = b"P1WAL001";
pub const WAL_HDR_SIZE: usize = 16;

// Формат записи WAL (v1):
// Layout:
// [type u8]          -- WAL_REC_*
// [flags u8]
// [reserved u16]
// [lsn u64]
// [page_id u64]      -- для page_image; для других типов 0
// [len u32]          -- payload length
// [crc32 u32]        -- CRC over header (except crc field) + payload
//
// Total header size = 1 + 1 + 2 + 8 + 8 + 4 + 4 = 28 bytes.
pub const WAL_REC_HDR_SIZE: usize = 28;

// Типы записей WAL:
pub const WAL_REC_PAGE_IMAGE: u8 = 1;
// Опциональный TRUNCATE-рекорд в стриме ship (payload=0).
// Старые клиенты проигнорируют как unknown.
pub const WAL_REC_TRUNCATE: u8 = 2;

// Offsets inside record header
pub const WAL_REC_OFF_TYPE: usize = 0;
pub const WAL_REC_OFF_FLAGS: usize = 1;
pub const WAL_REC_OFF_RESERVED: usize = 2;
pub const WAL_REC_OFF_LSN: usize = 4;
pub const WAL_REC_OFF_PAGE_ID: usize = 12;
pub const WAL_REC_OFF_LEN: usize = 20;
pub const WAL_REC_OFF_CRC32: usize = 24;

// Порог очистки WAL (байт): при превышении — truncate до заголовка
pub const WAL_ROTATE_SIZE: u64 = 8 * 1024 * 1024;