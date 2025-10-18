use anyhow::Result;
use std::path::PathBuf;

use QuiverDB::db::Db;

/// CLI: exists — быстрый presence‑check (использует Bloom fast‑path, если фильтр свежий).
pub fn exec(path: PathBuf, key: String) -> Result<()> {
    let db = Db::open_ro(&path)?;
    let present = db.exists(key.as_bytes())?;
    if present {
        println!("FOUND '{}'", key);
    } else {
        println!("NOT FOUND '{}'", key);
    }
    Ok(())
}
