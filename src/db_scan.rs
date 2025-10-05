//! High-level scan helpers for Db: scan_all and scan_prefix.
//!
//! Goals:
//! - Provide simple developer-friendly APIs to iterate all KV pairs
//!   or only those with a given prefix.
//! - Keep semantics consistent with Db::get (tail wins): if a key appears
//!   multiple times along a bucket chain, the last occurrence (closer to tail)
//!   overrides earlier ones.
//!
//! Notes:
//! - This implementation materializes results into Vec<(Vec<u8>, Vec<u8>)>.
//!   It's simple and good for tooling and integration; a streaming iterator
//!   can be added later if needed.

use anyhow::{anyhow, Result};
use std::collections::HashMap;

use crate::consts::NO_PAGE;
use crate::page_rh::{rh_header_read, rh_kv_list, rh_page_is_kv};
use crate::page_ovf::{ovf_parse_placeholder, ovf_read_chain};

impl crate::db::Db {
    /// Collect all key/value pairs across all buckets and chains.
    /// For keys that appear multiple times in a chain, the last one (tail-most) wins.
    pub fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_internal(None)
    }

    /// Collect key/value pairs for keys that start with the given prefix.
    /// For keys that appear multiple times in a chain, the last one (tail-most) wins.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_internal(Some(prefix))
    }

    fn scan_internal(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let ps = self.pager.meta.page_size as usize;
        let mut page_buf = vec![0u8; ps];

        // We'll collect into a map; inserting in head->tail order ensures
        // that tail-most value overwrites earlier ones (tail wins).
        let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        for b in 0..self.dir.bucket_count {
            let mut pid = self.dir.head(b)?;
            while pid != NO_PAGE {
                self.pager.read_page(pid, &mut page_buf)?;
                if !rh_page_is_kv(&page_buf) {
                    return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
                }
                let h = rh_header_read(&page_buf)?;

                if let Ok(items) = rh_kv_list(&page_buf) {
                    for (k, v) in items {
                        if let Some(pref) = prefix {
                            if !k.starts_with(pref) {
                                continue;
                            }
                        }
                        // Resolve overflow placeholder if present
                        let value_bytes = if let Some((total_len, head_pid)) = ovf_parse_placeholder(&v) {
                            ovf_read_chain(&self.pager, head_pid, Some(total_len as usize))?
                        } else {
                            v.to_vec()
                        };
                        map.insert(k.to_vec(), value_bytes);
                    }
                }

                pid = h.next_page_id;
            }
        }

        // Materialize in arbitrary order. If ordering is needed later,
        // we can add sorting or stable iteration.
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(map.len());
        for (k, v) in map {
            out.push((k, v));
        }
        Ok(out)
    }
}