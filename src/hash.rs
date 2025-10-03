//! Stable hashing utilities for keys and buckets.
//!
//! Goals:
//! - Use a stable, explicit hash (not std::DefaultHasher) to keep bucket mapping
//!   invariant across toolchains/platforms.
//! - Encode hash kind into meta for forward compatibility.

use std::fmt;
use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

/// Type of stable hash used by the database.
/// Store as u32 in meta for forward/backward compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashKind {
    /// 64-bit xxhash with seed=0. Fast and stable.
    Xx64Seed0 = 1,
}

impl HashKind {
    /// Convert to a compact u32 code for on-disk storage.
    pub fn to_u32(self) -> u32 {
        match self {
            HashKind::Xx64Seed0 => 1,
        }
    }

    /// Parse from on-disk u32 code. Unknown codes return None.
    pub fn from_u32(code: u32) -> Option<Self> {
        match code {
            1 => Some(HashKind::Xx64Seed0),
            _ => None,
        }
    }
}

impl fmt::Display for HashKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HashKind::Xx64Seed0 => write!(f, "xxhash64(seed=0)"),
        }
    }
}

/// Default hash kind for new databases.
pub const HASH_KIND_DEFAULT: HashKind = HashKind::Xx64Seed0;

/// Compute 64-bit stable hash of a key for given kind.
pub fn hash64(kind: HashKind, key: &[u8]) -> u64 {
    match kind {
        HashKind::Xx64Seed0 => {
            let mut h = XxHash64::with_seed(0);
            key.hash(&mut h);
            h.finish()
        }
    }
}

/// Compute bucket index from a 64-bit hash value.
#[inline]
pub fn bucket_index(hash: u64, buckets: u32) -> u32 {
    debug_assert!(buckets > 0, "buckets must be > 0");
    (hash % (buckets as u64)) as u32
}

/// Stable mapping from key -> bucket using the selected hash kind.
#[inline]
pub fn bucket_of_key(kind: HashKind, key: &[u8], buckets: u32) -> u32 {
    let h = hash64(kind, key);
    bucket_index(h, buckets)
}

/// Convenience wrapper that uses the crate-wide default hash kind.
#[inline]
pub fn bucket_of_key_default(key: &[u8], buckets: u32) -> u32 {
    bucket_of_key(HASH_KIND_DEFAULT, key, buckets)
}

/// A short non-zero 8-bit fingerprint derived from a 64-bit hash.
/// Useful for in-page hash tables to quickly filter candidates.
/// Returns 1..=255 (never 0) to allow 0 as "empty" sentinel in tables.
#[inline]
pub fn short_fingerprint_u8(hash: u64) -> u8 {
    let fp = (hash >> 56) as u8;
    if fp == 0 { 1 } else { fp }
}