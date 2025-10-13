use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use std::path::PathBuf;

use QuiverDB::db::Db;

use super::util::decode_value_arg;

#[derive(Debug, Deserialize)]
struct RawOp {
    op: String,
    key: String,
    value: Option<String>,
}

pub fn exec(path: PathBuf, ops_file: Option<PathBuf>, ops_json: Option<String>) -> Result<()> {
    let raw = if let Some(p) = ops_file {
        std::fs::read_to_string(&p).with_context(|| format!("read ops file {}", p.display()))?
    } else if let Some(s) = ops_json {
        s
    } else {
        return Err(anyhow!("provide --ops-file or --ops-json"));
    };

    let ops: Vec<RawOp> = serde_json::from_str(&raw).context("parse ops json (array of objects)")?;
    if ops.is_empty() {
        println!("No ops to execute.");
        return Ok(());
    }

    let mut db = Db::open(&path)?;
    db.batch(|b| {
        for op in ops {
            match op.op.to_ascii_lowercase().as_str() {
                "put" => {
                    let v = op.value.ok_or_else(|| anyhow!("put requires value for key '{}'", op.key))?;
                    let (bytes, _src) = decode_value_arg(&v)?;
                    b.put(op.key.as_bytes(), &bytes)?;
                }
                "del" => {
                    let _ = b.del(op.key.as_bytes())?;
                }
                other => return Err(anyhow!("unknown op '{}'", other)),
            }
        }
        Ok(())
    })?;

    println!("Batch: OK");
    Ok(())
}