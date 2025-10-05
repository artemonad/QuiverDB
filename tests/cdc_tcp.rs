use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};

use QuiverDB::{init_db, read_meta, Directory};
use QuiverDB::cli::cdc::{cmd_wal_ship_ext, wal_apply_from_stream};
use QuiverDB::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32,
    WAL_REC_OFF_LEN, WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE,
};
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{rh_kv_lookup, rh_page_is_kv};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let base = std::env::temp_dir();
    base.join(format!("qdbtest-cdc-tcp-{prefix}-{pid}-{t}-{id}"))
}

fn parse_wal_frames(root: &PathBuf) -> Result<Vec<(u64, u64)>> {
    // Возвращаем список кадров (lsn, page_id) для PAGE_IMAGE.
    use std::fs::OpenOptions;
    let wal_path = root.join(WAL_FILE);
    let mut f = OpenOptions::new().read(true).open(&wal_path)?;
    let mut hdr16 = vec![0u8; WAL_HDR_SIZE];
    f.read_exact(&mut hdr16)?;
    assert_eq!(&hdr16[..8], WAL_MAGIC, "bad WAL magic");
    let len = f.metadata()?.len();
    let mut pos = WAL_HDR_SIZE as u64;
    let mut out = Vec::new();
    while pos + (WAL_REC_HDR_SIZE as u64) <= len {
        f.seek(SeekFrom::Start(pos))?;
        let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
        if f.read_exact(&mut hdr).is_err() {
            break;
        }
        let payload_len =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4]) as usize;
        let _crc_expected =
            LittleEndian::read_u32(&hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4]);
        let rec_total = WAL_REC_HDR_SIZE as u64 + (payload_len as u64);
        if pos + rec_total > len {
            break;
        }
        let mut payload = vec![0u8; payload_len];
        f.read_exact(&mut payload)?;
        let rec_type = hdr[0];
        let lsn = LittleEndian::read_u64(&hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8]);
        if rec_type == WAL_REC_PAGE_IMAGE {
            let page_id =
                LittleEndian::read_u64(&hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8]);
            out.push((lsn, page_id));
        }
        pos += rec_total;
    }
    Ok(out)
}

// Подбираем две строки, которые гарантированно попадают в разные бакеты.
fn two_keys_in_different_buckets(dir: &Directory) -> (Vec<u8>, Vec<u8>) {
    let base1 = b"ka";
    let mut k1 = base1.to_vec();
    for i in 0..256u16 {
        k1.clear();
        k1.extend_from_slice(base1);
        k1.extend_from_slice(&i.to_be_bytes());
        let b1 = dir.bucket_of_key(&k1);
        // Ищем вторую, с другим bucket
        let base2 = b"kb";
        let mut k2 = base2.to_vec();
        for j in 0..256u16 {
            k2.clear();
            k2.extend_from_slice(base2);
            k2.extend_from_slice(&j.to_be_bytes());
            let b2 = dir.bucket_of_key(&k2);
            if b1 != b2 {
                return (k1.clone(), k2.clone());
            }
        }
    }
    // Fallback
    (b"a".to_vec(), b"z".to_vec())
}

// Скан всех RH-страниц фолловера: ищем ключ (без directory).
fn follower_has_key(root: &PathBuf, key: &[u8]) -> Result<bool> {
    let pager = Pager::open(root)?;
    let ps = pager.meta.page_size as usize;
    let mut page = vec![0u8; ps];

    for pid in 0..pager.meta.next_page_id {
        if pager.read_page(pid, &mut page).is_ok() {
            if rh_page_is_kv(&page) {
                if let Ok(v) = rh_kv_lookup(&page, pager.meta.hash_kind, key) {
                    if v.is_some() {
                        return Ok(true);
                    }
                }
            }
        }
    }
    Ok(false)
}

#[test]
fn cdc_tcp_full_stream_applies_on_follower() -> Result<()> {
    // leader + follower
    let leader = unique_root("leader-full");
    let follower = unique_root("follower-full");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    init_db(&follower, 4096)?;
    Directory::create(&leader, 64)?;

    // Пишем два ключа в разные бакеты
    {
        use QuiverDB::Db;
        let dir = Directory::open(&leader)?;
        let (k1, k2) = two_keys_in_different_buckets(&dir);
        let b1 = dir.bucket_of_key(&k1);
        let b2 = dir.bucket_of_key(&k2);
        println!("full_stream buckets: b1={}, b2={}", b1, b2);
        assert_ne!(b1, b2);

        let mut db = Db::open(&leader)?;
        db.put(&k1, b"v1")?;
        db.put(&k2, b"v2")?;
    }

    // TCP listener + applier
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let follower_clone = follower.clone();
    let (tx, rx) = mpsc::channel();
    let t = thread::spawn(move || -> Result<()> {
        tx.send(()).ok();
        let (mut stream, _) = listener.accept()?;
        wal_apply_from_stream(&follower_clone, &mut stream)?;
        Ok(())
    });
    let _ = rx.recv();

    // Ship весь WAL в TCP
    cmd_wal_ship_ext(leader.clone(), false, None, Some(format!("tcp://{}", addr)))?;
    t.join().unwrap()?;

    // Проверяем: у фолловера есть оба ключа
    let dir = Directory::open(&leader)?;
    let (k1, k2) = two_keys_in_different_buckets(&dir);
    assert!(follower_has_key(&follower, &k1)?, "k1 missing on follower");
    assert!(follower_has_key(&follower, &k2)?, "k2 missing on follower");
    Ok(())
}

#[test]
fn cdc_tcp_since_lsn_sends_only_tail_frames() -> Result<()> {
    let leader = unique_root("leader-since");
    let follower = unique_root("follower-since");
    fs::create_dir_all(&leader)?;
    fs::create_dir_all(&follower)?;
    init_db(&leader, 4096)?;
    init_db(&follower, 4096)?;
    Directory::create(&leader, 64)?;

    use QuiverDB::Db;

    // Подберём два ключа в разных бакетах
    let dir = Directory::open(&leader)?;
    let (k1, k2) = two_keys_in_different_buckets(&dir);
    let b1 = dir.bucket_of_key(&k1);
    let b2 = dir.bucket_of_key(&k2);
    println!("since_lsn buckets: b1={}, b2={}", b1, b2);
    assert_ne!(b1, b2, "keys must be in different buckets");

    // 1) Запишем первый ключ и зафиксируем lsn1
    {
        let mut db = Db::open(&leader)?;
        db.put(&k1, b"v1")?;
    }
    let m1 = read_meta(&leader)?;
    let lsn1 = m1.last_lsn;
    println!("lsn1 (after k1) = {}", lsn1);

    // 2) Запишем второй ключ (должен дать lsn2 > lsn1)
    {
        let mut db = Db::open(&leader)?;
        db.put(&k2, b"v2")?;
    }
    let m2 = read_meta(&leader)?;
    let lsn2 = m2.last_lsn;
    println!("lsn2 (after k2) = {}", lsn2);
    assert!(lsn2 > lsn1, "lsn2 must be > lsn1");

    // Диагностика: список кадров в текущем WAL.
    // На чистом старте между k1 и k2 WAL мог быть обрезан (clean startup) — поэтому сейчас
    // ожидаем увидеть хотя бы кадр с lsn2.
    let frames = parse_wal_frames(&leader)?;
    println!("WAL frames (lsn,page_id): {:?}", frames);
    assert!(
        frames.iter().any(|(lsn, _)| *lsn == lsn2),
        "WAL must contain a frame with lsn2={}",
        lsn2
    );

    // TCP listener + applier
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let follower_clone = follower.clone();
    let (tx, rx) = mpsc::channel();
    let t = thread::spawn(move || -> Result<()> {
        tx.send(()).ok();
        let (mut stream, _) = listener.accept()?;
        wal_apply_from_stream(&follower_clone, &mut stream)?;
        Ok(())
    });
    let _ = rx.recv();

    // Шлём только записи с lsn > lsn1
    cmd_wal_ship_ext(leader.clone(), false, Some(lsn1), Some(format!("tcp://{}", addr)))?;
    t.join().unwrap()?;

    // Ожидаем: у фолловера должен быть k2, а k1 — отсутствовать
    assert!(
        !follower_has_key(&follower, &k1)?,
        "k1 must not be present on follower when shipping only lsn>lsn1"
    );
    assert!(
        follower_has_key(&follower, &k2)?,
        "k2 must be present on follower after since_lsn ship"
    );

    Ok(())
}