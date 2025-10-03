use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;

use QuiverDB::{init_db, Db, Directory};
use QuiverDB::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE,
};

// ---------- helpers ----------

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-v05-{prefix}-{pid}-{t}-{id}"))
}

// ---------- tests ----------

#[test]
fn wal_tail_like_parsing_and_lsn_monotonicity() {
    // 1) Подготовка БД и запись нескольких ключей
    let root = unique_root("wal-tail");
    std::fs::create_dir_all(&root).expect("create root dir");
    init_db(&root, 4096).expect("init_db");
    Directory::create(&root, 64).expect("dir create");

    {
        let mut db = Db::open(&root).expect("db open");
        for i in 0..10 {
            let k = format!("k{i:03}");
            let v = format!("v{i:03}");
            db.put(k.as_bytes(), v.as_bytes()).expect("put");
        }
        // drop -> clean_shutdown=true (в Db::drop)
    }

    // 2) Чтение WAL и проверка кадров (как в wal-tail)
    let wal_path = root.join(WAL_FILE);
    let mut f = OpenOptions::new()
        .read(true)
        .open(&wal_path)
        .expect("open wal");

    // Заголовок
    assert!(
        f.metadata().expect("meta").len() >= WAL_HDR_SIZE as u64,
        "wal size must be >= header"
    );
    let mut magic = [0u8; 8];
    f.seek(SeekFrom::Start(0)).expect("seek 0");
    f.read_exact(&mut magic).expect("read magic");
    assert_eq!(&magic, WAL_MAGIC, "bad WAL magic");

    // Парсинг кадров
    let mut pos = WAL_HDR_SIZE as u64;
    let len = f.metadata().expect("meta2").len();
    let mut lsns: Vec<u64> = Vec::new();
    let mut page_ids: Vec<u64> = Vec::new();

    while pos + (WAL_REC_HDR_SIZE as u64) <= len {
        f.seek(SeekFrom::Start(pos)).expect("seek hdr");
        let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
        if f.read_exact(&mut hdr).is_err() {
            break;
        }
        let rec_type = hdr[0];
        let payload_len =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
        let crc_expected =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4]);
        let rec_total = WAL_REC_HDR_SIZE as u64 + payload_len as u64;
        if pos + rec_total > len {
            break; // хвост ещё пишется
        }
        let mut payload = vec![0u8; payload_len];
        f.read_exact(&mut payload).expect("read payload");

        // CRC
        let mut hasher = Crc32::new();
        hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        assert_eq!(crc_actual, crc_expected, "CRC mismatch in WAL record");

        let lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);
        lsns.push(lsn);

        if rec_type == WAL_REC_PAGE_IMAGE {
            let page_id =
                LittleEndian::read_u64(&hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8]);
            page_ids.push(page_id);
            // payload — полный образ страницы (не проверяем тут содержимое)
        }

        pos += rec_total;
    }

    // Должны быть записи
    assert!(
        !lsns.is_empty(),
        "WAL must contain at least one page_image record after puts"
    );

    // LSN должен монотонно возрастать
    for w in lsns.windows(2) {
        assert!(w[0] < w[1], "LSN must be strictly increasing: {:?}",
                lsns);
    }

    // page_id — неотрицательные, а в целом их может быть немного (1-2 или больше)
    assert!(
        page_ids.iter().all(|&pid| pid < u64::MAX),
        "page_id must be a valid value"
    );
}