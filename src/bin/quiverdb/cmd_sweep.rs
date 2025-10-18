use anyhow::Result;
use std::path::PathBuf;

use QuiverDB::db::Db;

pub fn exec(path: PathBuf) -> Result<()> {
    let mut db = Db::open(&path)?;
    let freed = db.sweep_orphan_overflow()?;
    println!("Sweep: freed {} orphan OVERFLOW page(s)", freed);
    Ok(())
}
