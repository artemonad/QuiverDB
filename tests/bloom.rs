use anyhow::Result;
use std::fs;

use QuiverDB::bloom::BloomSidecar;
use QuiverDB::db::Db;

/// Bloom: rebuild v2 + свежесть + отрицательный хинт.
#[test]
fn bloom_fresh_negative_hint() -> Result<()> {
    let root = unique_root("bloom");
    fs::create_dir_all(&root)?;

    // init
    let page_size = 64 * 1024;
    let buckets = 64;
    Db::init(&root, page_size, buckets)?;

    // 1) Пишем пару ключей
    {
        let mut db = Db::open(&root)?;
        db.put(b"alpha", b"1")?;
        db.put(b"beta", b"2")?;
    }

    // 2) Строим bloom.bin и делаем полный ребилд
    let db_ro = Db::open_ro(&root)?;
    let mut sidecar = BloomSidecar::open_or_create_for_db(&db_ro, 4096, 6)?;
    sidecar.rebuild_all(&db_ro)?;

    // 3) Bloom должен быть "свежим" (last_lsn совпадает)
    assert!(
        sidecar.is_fresh_for_db(&db_ro),
        "bloom must be fresh after full rebuild (last_lsn match)"
    );

    // 4) Отрицательный хинт: ключа "missing" точно нет
    let bucket = db_ro.dir.bucket_of_key(b"missing", db_ro.pager.meta.hash_kind);
    let maybe = sidecar.test(bucket, b"missing")?;
    assert!(!maybe, "bloom must say 'definitely absent' for missing");

    // 5) Db::get("missing") → None (fast-path или обычный обход — корректный результат)
    let got = db_ro.get(b"missing")?;
    assert!(got.is_none(), "missing key must be None");

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