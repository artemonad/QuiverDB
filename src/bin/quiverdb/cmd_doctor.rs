use anyhow::Result;
use std::path::PathBuf;

use QuiverDB::db::Db;

pub fn exec(path: PathBuf, json: bool) -> Result<()> {
    let db = Db::open_ro(&path)?;
    db.doctor(json)
}