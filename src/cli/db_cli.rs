use anyhow::{anyhow, Result};
use std::path::PathBuf;

use crate::db::Db;
use crate::dir::Directory;
use crate::meta::read_meta;
use crate::util::{display_text, hex_dump};
use crate::init_db;

pub fn cmd_db_init(path: PathBuf, page_size: u32, buckets: u32) -> Result<()> {
    if !path.exists() || !path.join("meta").exists() {
        init_db(&path, page_size)?;
        println!("Initialized DB at {}", path.display());
    } else {
        let meta = read_meta(&path)?;
        if meta.page_size != page_size {
            println!(
                "Warning: meta.page_size={} differs from requested {}. Using {}",
                meta.page_size, page_size, meta.page_size
            );
        }
    }
    if path.join("dir").exists() {
        return Err(anyhow!("Directory already exists at {}/dir", path.display()));
    }
    Directory::create(&path, buckets)?;
    println!("Created directory with {} buckets", buckets);
    Ok(())
}

pub fn cmd_db_put(path: PathBuf, key: String, value: String) -> Result<()> {
    let mut db = Db::open(&path)?;
    db.put(key.as_bytes(), value.as_bytes())?;
    println!("OK");
    Ok(())
}

pub fn cmd_db_get(path: PathBuf, key: String) -> Result<()> {
    let db = Db::open(&path)?;
    match db.get(key.as_bytes())? {
        Some(v) => {
            println!("FOUND '{}': {}", key, display_text(&v));
            println!("hex: {}", hex_dump(&v[..v.len().min(64)]));
        }
        None => println!("NOT FOUND '{}'", key),
    }
    Ok(())
}

pub fn cmd_db_del(path: PathBuf, key: String) -> Result<()> {
    let mut db = Db::open(&path)?;
    let existed = db.del(key.as_bytes())?;
    if existed {
        println!("DELETED '{}'", key);
    } else {
        println!("NOT FOUND '{}'", key);
    }
    Ok(())
}

pub fn cmd_db_stats(path: PathBuf) -> Result<()> {
    let db = Db::open(&path)?;
    db.print_stats()
}