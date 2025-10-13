use anyhow::Result;
use std::path::PathBuf;

use QuiverDB::db::Db;
use QuiverDB::dir::Directory;
use QuiverDB::meta::read_meta;

pub fn exec(path: PathBuf, page_size: u32, buckets: u32) -> Result<()> {
    if !path.exists() {
        std::fs::create_dir_all(&path)?;
    }
    let meta_path = path.join("meta");
    if meta_path.exists() {
        let m = read_meta(&path)?;
        if m.page_size != page_size {
            eprintln!(
                "warning: DB already initialized with page_size={}, requested {} (keeping {})",
                m.page_size, page_size, m.page_size
            );
        }
        match Directory::open(&path) {
            Ok(_) => println!("DB already initialized at {}", path.display()),
            Err(_) => {
                Directory::create(&path, buckets)?;
                println!("Created directory with {} buckets at {}", buckets, path.display());
            }
        }
        return Ok(());
    }
    Db::init(&path, page_size, buckets)?;
    println!("Initialized DB at {}", path.display());
    Ok(())
}