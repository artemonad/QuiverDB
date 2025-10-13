use anyhow::{anyhow, Result};
use std::path::PathBuf;

use QuiverDB::db::Db;

use super::util::{decode_value_arg, read_all};

pub fn exec(path: PathBuf, key: String, value: Option<String>, value_file: Option<PathBuf>) -> Result<()> {
    let val_bytes = match (value, value_file) {
        (_, Some(p)) => read_all(&p)?,
        (Some(s), None) => decode_value_arg(&s)?.0,
        (None, None) => return Err(anyhow!("either --value or --value-file must be provided")),
    };

    let mut db = Db::open(&path)?;
    db.put(key.as_bytes(), &val_bytes)?;
    println!(
        "OK put: key='{}' ({} B), value={} B",
        key,
        key.as_bytes().len(),
        val_bytes.len()
    );
    Ok(())
}