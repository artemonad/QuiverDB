use crate::consts::{DIR_FILE, DIR_HDR_SIZE, DIR_MAGIC, NO_PAGE};
use crate::hash::{bucket_of_key, HashKind};
use crate::meta::read_meta;
use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt}; // NOTE: ByteOrder added
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub struct Directory {
    pub path: PathBuf,
    pub bucket_count: u32,
    pub hash_kind: HashKind,
}

impl Directory {
    pub fn create(root: &Path, buckets: u32) -> Result<Self> {
        if buckets == 0 {
            return Err(anyhow!("buckets must be > 0"));
        }
        // meta уже должна существовать — берём из неё выбранный стабильный хеш
        let meta = read_meta(root).context("read meta for directory create")?;
        let path = root.join(DIR_FILE);
        let mut f = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)
            .with_context(|| format!("create dir {}", path.display()))?;

        // header
        f.write_all(DIR_MAGIC)?;
        f.write_u32::<LittleEndian>(1)?; // version
        f.write_u32::<LittleEndian>(buckets)?;
        f.write_u64::<LittleEndian>(0)?; // reserved

        // heads
        for _ in 0..buckets {
            f.write_u64::<LittleEndian>(NO_PAGE)?;
        }
        f.sync_all()?;
        Ok(Self {
            path,
            bucket_count: buckets,
            hash_kind: meta.hash_kind,
        })
    }

    pub fn open(root: &Path) -> Result<Self> {
        let meta = read_meta(root).context("read meta for directory open")?;
        let path = root.join(DIR_FILE);
        let mut f = OpenOptions::new()
            .read(true)
            .open(&path)
            .with_context(|| format!("open dir {}", path.display()))?;
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        if &magic != DIR_MAGIC {
            return Err(anyhow!("bad DIR magic in {}", path.display()));
        }
        let _version = f.read_u32::<LittleEndian>()?;
        let buckets = f.read_u32::<LittleEndian>()?;
        let _reserved = f.read_u64::<LittleEndian>()?;
        Ok(Self {
            path,
            bucket_count: buckets,
            hash_kind: meta.hash_kind,
        })
    }

    pub fn head(&self, bucket: u32) -> Result<u64> {
        if bucket >= self.bucket_count {
            return Err(anyhow!(
                "bucket {} out of range 0..{}",
                bucket,
                self.bucket_count - 1
            ));
        }
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        let off = DIR_HDR_SIZE as u64 + (bucket as u64) * 8;
        f.seek(SeekFrom::Start(off))?;
        let mut buf = [0u8; 8];
        f.read_exact(&mut buf)?;
        Ok(LittleEndian::read_u64(&buf))
    }

    pub fn set_head(&self, bucket: u32, page_id: u64) -> Result<()> {
        if bucket >= self.bucket_count {
            return Err(anyhow!(
                "bucket {} out of range 0..{}",
                bucket,
                self.bucket_count - 1
            ));
        }
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)?;
        let off = DIR_HDR_SIZE as u64 + (bucket as u64) * 8;
        f.seek(SeekFrom::Start(off))?;
        f.write_u64::<LittleEndian>(page_id)?;
        f.sync_all()?;
        Ok(())
    }

    pub fn bucket_of_key(&self, key: &[u8]) -> u32 {
        bucket_of_key(self.hash_kind, key, self.bucket_count)
    }

    pub fn count_used_buckets(&self) -> Result<u32> {
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        f.seek(SeekFrom::Start(DIR_HDR_SIZE as u64))?;
        let mut used = 0u32;
        for _ in 0..self.bucket_count {
            let pid = f.read_u64::<LittleEndian>()?;
            if pid != NO_PAGE {
                used += 1;
            }
        }
        Ok(used)
    }
}