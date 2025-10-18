use anyhow::Result;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

use QuiverDB::db::Db;

use super::util::{display_text, hex_dump};

pub fn exec(path: PathBuf, key: String, out: Option<PathBuf>) -> Result<()> {
    let db = Db::open_ro(&path)?;
    match db.get(key.as_bytes())? {
        Some(v) => {
            if let Some(out_path) = out {
                if let Some(parent) = out_path.parent() {
                    if !parent.as_os_str().is_empty() {
                        std::fs::create_dir_all(parent)?;
                    }
                }
                let mut f = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&out_path)?;
                f.write_all(&v)?;
                f.sync_all()?;
                println!(
                    "FOUND '{}': {} B -> wrote to {}",
                    key,
                    v.len(),
                    out_path.display()
                );
            } else {
                println!("FOUND '{}': {} B", key, v.len());
                println!("text: {}", display_text(&v));
                println!("hex:  {}", hex_dump(&v[..v.len().min(64)]));
            }
        }
        None => println!("NOT FOUND '{}'", key),
    }
    Ok(())
}
