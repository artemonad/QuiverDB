// Общие (meta)
pub const MAGIC: &[u8; 8] = b"P1DBMETA";
pub const META_FILE: &str = "meta";

// Данные (сегменты)
pub const DATA_SEG_PREFIX: &str = "data-";
pub const DATA_SEG_EXT: &str = "p1seg";
pub const SEGMENT_SIZE: u64 = 32 * 1024 * 1024;

// Флаги (зарезервировано под будущие фичи)
pub const FLAG_TDE_ENABLED: u32 = 0x1;

// Страницы (только v2: Robin Hood)
pub const PAGE_MAGIC: &[u8; 4] = b"P1PG";
pub const PAGE_TYPE_KV_RH: u16 = 2;
pub const PAGE_HDR_V2_SIZE: usize = 64; // [MAGIC4][ver u16=2][type u16][pad4][page_id u64][data_start u16][table_slots u16][used_slots u16][flags u16][next_page_id u64][lsn u64][seed u64][crc32 u32]

pub const NO_PAGE: u64 = u64::MAX;

// Каталог
pub const DIR_FILE: &str = "dir";
pub const DIR_MAGIC: &[u8; 8] = b"P1DIR001";
pub const DIR_HDR_SIZE: usize = 24; // [magic8][ver u32][buckets u32][reserved u64]

// WAL
pub const WAL_FILE: &str = "wal-000001.log";
pub const WAL_MAGIC: &[u8; 8] = b"P1WAL001";
pub const WAL_HDR_SIZE: usize = 16;

// WAL record with LSN (v2):
// Layout:
// [type u8]
// [flags u8]
// [reserved u16]
// [lsn u64]
// [page_id u64]
// [len u32]         -- payload length (page image)
// [crc32 u32]       -- CRC over header (except crc field) + payload
//
// Total header size = 1 + 1 + 2 + 8 + 8 + 4 + 4 = 28 bytes.
pub const WAL_REC_HDR_SIZE: usize = 28;
pub const WAL_REC_PAGE_IMAGE: u8 = 1;

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