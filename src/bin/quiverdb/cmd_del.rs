use anyhow::Result;
use std::path::PathBuf;

use QuiverDB::db::Db;

pub fn exec(path: PathBuf, key: String) -> Result<()> {
    let mut db = Db::open(&path)?;
    let existed = db.del(key.as_bytes())?;
    if existed {
        println!("DELETED '{}'", key);
    } else {
        println!("DELETE requested, but head was empty for '{}'", key);
    }
    Ok(())
}