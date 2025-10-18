use anyhow::Result;
use std::path::PathBuf;

use QuiverDB::db::Db;

use super::util::{display_text, to_hex};

pub fn exec(path: PathBuf, prefix: Option<String>, json: bool, stream: bool) -> Result<()> {
    let db = Db::open_ro(&path)?;
    let pref_bytes = prefix.as_ref().map(|s| s.as_bytes());

    if stream {
        if json {
            db.scan_stream(pref_bytes, |k, v| {
                println!(
                    "{{\"key_hex\":\"{}\",\"value_hex\":\"{}\",\"key_len\":{},\"value_len\":{}}}",
                    to_hex(k),
                    to_hex(v),
                    k.len(),
                    v.len()
                );
            })?;
        } else {
            db.scan_stream(pref_bytes, |k, v| {
                println!(
                    "key='{}' ({} B) -> value '{}' ({} B)",
                    display_text(k),
                    k.len(),
                    display_text(v),
                    v.len()
                );
            })?;
        }
        return Ok(());
    }

    let mut acc: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    db.scan_stream(pref_bytes, |k, v| {
        acc.push((k.to_vec(), v.to_vec()));
    })?;

    if json {
        print!("[");
        for (i, (k, v)) in acc.iter().enumerate() {
            if i > 0 {
                print!(",");
            }
            print!(
                "{{\"key_hex\":\"{}\",\"value_hex\":\"{}\",\"key_len\":{},\"value_len\":{}}}",
                to_hex(k),
                to_hex(v),
                k.len(),
                v.len()
            );
        }
        println!("]");
    } else {
        if acc.is_empty() {
            println!("(no items)");
        } else {
            for (k, v) in acc {
                println!(
                    "key='{}' ({} B) -> value '{}' ({} B)",
                    display_text(&k),
                    k.len(),
                    display_text(&v),
                    v.len()
                );
            }
        }
    }

    Ok(())
}
