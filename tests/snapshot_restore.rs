use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::dir::Directory;
use QuiverDB::meta::read_meta;
use QuiverDB::snapstore::{read_manifest, restore_from_id, SnapshotManager};
use QuiverDB::wal::{WAL_FILE, WAL_HDR_SIZE};

#[test]
fn snapshot_restore_roundtrip() -> Result<()> {
    // Подготовим исходную БД
    let src = unique_root("snap-restore-src");
    fs::create_dir_all(&src)?;

    let page_size = 64 * 1024;
    let buckets = 64;
    Db::init(&src, page_size, buckets)?;

    // Данные: малое, большое и tombstone
    let big_len = page_size as usize + 777;
    let big = build_bytes(big_len, 0xAB, 0x11);

    {
        let mut dbw = Db::open(&src)?;
        dbw.batch(|b| {
            b.put(b"k_small", b"v1")?;
            b.put(b"k_big", &big)?;
            Ok(())
        })?;
        // tombstone после
        let _ = dbw.del(b"k_deleted")?;
    }

    // Создадим persisted‑снапшот
    let db_ro = Db::open_ro(&src)?;
    let snap_id = SnapshotManager::create_persisted(&db_ro, Some("restore-e2e"), &[], None)?;
    // Манифест существует
    let _m = read_manifest(&src, &snap_id)?;

    // Восстановим в новый корень
    let dst = unique_root("snap-restore-dst");
    fs::create_dir_all(&dst)?;

    restore_from_id(&src, &dst, &snap_id, true /*verify*/)?;

    // Проверка содержимого на стороне dst
    {
        // Статус meta/dir
        let m = read_meta(&dst)?;
        assert_eq!(m.page_size, page_size);
        assert!(m.clean_shutdown, "restored DB must be clean_shutdown");

        let dir = Directory::open(&dst)?;
        assert_eq!(dir.bucket_count, buckets);

        // WAL усечён до заголовка
        let wal_len = fs::metadata(dst.join(WAL_FILE))?.len();
        assert_eq!(
            wal_len, WAL_HDR_SIZE as u64,
            "WAL must be truncated to header"
        );

        // Ключи
        let dbr = Db::open_ro(&dst)?;
        // малый
        let s = dbr.get(b"k_small")?.expect("k_small must exist");
        assert_eq!(s.as_slice(), b"v1");
        // большой
        let b2 = dbr.get(b"k_big")?.expect("k_big must exist");
        assert_eq!(b2.len(), big.len());
        assert_eq!(b2, big);
        // tombstone
        assert!(
            dbr.get(b"k_deleted")?.is_none(),
            "tombstone key must be absent after restore"
        );
    }

    Ok(())
}

// ---------- helpers ----------

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}

fn build_bytes(len: usize, fill: u8, mid_xor: u8) -> Vec<u8> {
    let mut v = vec![fill; len];
    if len >= 3 {
        v[0] = fill;
        v[len / 2] = fill ^ mid_xor;
        v[len - 1] = fill;
    }
    v
}
