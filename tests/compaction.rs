use anyhow::Result;
use std::fs;

use QuiverDB::db::Db;

/// Компактация: проверяем, что tombstone-ключи удаляются, валидные значения остаются,
/// а большие значения (OVERFLOW) читаются корректно после перестройки цепочек.
#[test]
fn compaction_basic_and_overflow() -> Result<()> {
    let root = unique_root("compaction");
    fs::create_dir_all(&root)?;

    // init
    let page_size = 64 * 1024;
    let buckets = 128;
    Db::init(&root, page_size, buckets)?;

    // 1) Пишем два маленьких ключа и одно большое значение
    let big_len = page_size as usize + 1024; // гарантированно OVERFLOW
    let big = build_pattern(big_len, 0xAB);

    {
        let mut db = Db::open(&root)?;
        db.put(b"k1", b"v1")?;
        db.put(b"k2", b"v2")?;
        db.put(b"big", &big)?;
        // Tombstone для k1
        let _ = db.del(b"k1")?;
    }

    // 2) До компактации: k1 отсутствует, k2 и big существуют
    {
        let db_ro = Db::open_ro(&root)?;
        let k1 = db_ro.get(b"k1")?;
        let k2 = db_ro.get(b"k2")?.expect("k2 must exist before compaction");
        let vb = db_ro.get(b"big")?.expect("big must exist before compaction");
        assert!(k1.is_none(), "k1 must be deleted by tombstone");
        assert_eq!(k2.as_slice(), b"v2");
        assert_eq!(vb.len(), big.len());
        assert_eq!(vb[0], 0xAB);
        assert_eq!(vb[vb.len() - 1], 0xAB);
    }

    // 3) Компактация всей БД
    let sum = {
        let mut db = Db::open(&root)?;
        db.compact_all()?
    };

    // Ожидания по отчёту: хотя бы один бакет был пересобран,
    // были записаны новые страницы (минимум одна).
    assert!(sum.buckets_compacted >= 1, "must compact at least 1 bucket");
    assert!(sum.pages_written_sum >= 1, "must write some pages during compaction");

    // 4) После компактации: k1 отсутствует, k2 = "v2", big совпадает по байтам
    {
        let db_ro = Db::open_ro(&root)?;
        let k1 = db_ro.get(b"k1")?;
        let k2 = db_ro.get(b"k2")?.expect("k2 must exist after compaction");
        let vb = db_ro.get(b"big")?.expect("big must exist after compaction");
        assert!(k1.is_none(), "k1 must remain deleted after compaction");
        assert_eq!(k2.as_slice(), b"v2");
        assert_eq!(vb, big, "big value content must match after compaction");
    }

    Ok(())
}

// ---------- helpers ----------

fn unique_root(prefix: &str) -> std::path::PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}

fn build_pattern(len: usize, byte: u8) -> Vec<u8> {
    let mut v = vec![byte; len];
    if len >= 8 {
        v[0] = byte;
        v[len / 2] = byte ^ 0x11;
        v[len - 1] = byte;
    }
    v
}