use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use QuiverDB::db::Db;

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}

// Проверяем P1_MAX_VALUE_BYTES guard на чтении OVERFLOW‑цепочки.
#[test]
fn overflow_guard_blocks_huge() -> Result<()> {
    let root = unique_root("ovf-guard");
    fs::create_dir_all(&root)?;
    // init
    let page_size = 64 * 1024;
    Db::init(&root, page_size, 32)?;

    // Ограничим максимально допустимый value до 1 KB
    std::env::set_var("P1_MAX_VALUE_BYTES", "1024");

    // Пишем value побольше, чтобы ушло в OVERFLOW (например 64 KiB * 2)
    let big_len = (page_size as usize) * 2;
    let big = vec![0xAB; big_len];

    {
        let mut db = Db::open(&root)?;
        db.put(b"big", &big)?;
    }

    // Ожидаем, что чтение теперь упадёт guard'ом
    let db_ro = Db::open_ro(&root)?;
    let err = db_ro.get(b"big").unwrap_err();
    let msg = format!("{:#}", err).to_ascii_lowercase();
    assert!(
        msg.contains("max_value_bytes") || msg.contains("exceeds"),
        "expected guard error, got: {msg}"
    );
    Ok(())
}
