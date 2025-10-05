use anyhow::Result;
use std::path::PathBuf;

use crate::meta::read_meta;

/// CDC helper: print meta.last_lsn for resume.
pub fn cmd_cdc_last_lsn(path: PathBuf) -> Result<()> {
    let m = read_meta(&path)?;
    println!("{}", m.last_lsn);
    Ok(())
}