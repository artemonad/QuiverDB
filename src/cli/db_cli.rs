// src/cli/db_cli.rs

use anyhow::{anyhow, Result};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::db::Db;
use crate::dir::Directory;
use crate::meta::read_meta;
use crate::util::{display_text, hex_dump};
use crate::init_db;

/// Простой HEX-декодер "aabbcc" -> [0xAA,0xBB,0xCC]
fn decode_hex(s: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err(anyhow!("hex string must be of even length"));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let h = (bytes[i] as char)
            .to_digit(16)
            .ok_or_else(|| anyhow!("invalid hex at pos {}", i))?;
        let l = (bytes[i + 1] as char)
            .to_digit(16)
            .ok_or_else(|| anyhow!("invalid hex at pos {}", i + 1))?;
        out.push(((h << 4) | l) as u8);
    }
    Ok(out)
}

/// Источник значения для put:
/// - "-"            -> stdin (raw bytes)
/// - "@<path>"      -> файл (raw bytes)
/// - "hex:<bytes>"  -> hex-декод
/// - иначе          -> строка как есть (UTF-8 bytes)
fn read_value_arg(arg: &str) -> Result<(Vec<u8>, &'static str)> {
    if arg == "-" {
        let mut buf = Vec::new();
        std::io::stdin().lock().read_to_end(&mut buf)?;
        return Ok((buf, "stdin"));
    }
    if let Some(p) = arg.strip_prefix('@') {
        let path = PathBuf::from(p);
        let mut f = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|e| anyhow!("open value file {}: {}", path.display(), e))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        return Ok((buf, "file"));
    }
    if let Some(hx) = arg.strip_prefix("hex:") {
        let v = decode_hex(hx)?;
        return Ok((v, "hex"));
    }
    // literal string
    Ok((arg.as_bytes().to_vec(), "literal"))
}

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
        return Err(anyhow!(
            "Directory already exists at {}/dir",
            path.display()
        ));
    }
    Directory::create(&path, buckets)?;
    println!("Created directory with {} buckets", buckets);
    Ok(())
}

pub fn cmd_db_put(path: PathBuf, key: String, value: String) -> Result<()> {
    // Расширенный парсинг аргумента value:
    // "-" / "@file" / "hex:..." / literal string
    let (val_bytes, src) = read_value_arg(&value)?;

    // Writer (exclusive): изменяет БД
    let mut db = Db::open(&path)?;
    db.put(key.as_bytes(), &val_bytes)?;
    println!(
        "OK (key='{}', value={} B, src={})",
        key,
        val_bytes.len(),
        src
    );
    Ok(())
}

pub fn cmd_db_get(path: PathBuf, key: String) -> Result<()> {
    // Reader-only (shared): не меняем clean_shutdown, не делаем replay
    let db = Db::open_ro(&path)?;
    match db.get(key.as_bytes())? {
        Some(v) => {
            // Если задана переменная окружения P1_DB_GET_OUT=<path> — пишем «сырые» байты в файл.
            if let Ok(out_path) = std::env::var("P1_DB_GET_OUT") {
                let out = PathBuf::from(out_path);
                if let Some(parent) = out.parent() {
                    if !parent.as_os_str().is_empty() {
                        let _ = fs::create_dir_all(parent);
                    }
                }
                let mut f = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&out)?;
                f.write_all(&v)?;
                f.sync_all()?;
                println!("FOUND '{}': {} B -> wrote to {}", key, v.len(), out.display());
            } else {
                // Стандартный вывод: текст + hex-превью
                println!("FOUND '{}': {} B: {}", key, v.len(), display_text(&v));
                println!("hex: {}", hex_dump(&v[..v.len().min(64)]));
            }
        }
        None => println!("NOT FOUND '{}'", key),
    }
    Ok(())
}

pub fn cmd_db_del(path: PathBuf, key: String) -> Result<()> {
    // Writer (exclusive)
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
    // Reader-only (shared)
    let db = Db::open_ro(&path)?;
    db.print_stats()
}