//! pager/core — ядро Pager: структура, open(), флаг data_fsync и общие помощники.

use anyhow::{anyhow, Context, Result};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};

use crate::crypto::{
    KeyProvider, KeyJournal, EnvKeyProvider,
    EnvKmsProvider, KmsProvider, // для метода unwrap()
    KeyRing,                     // стор обёрнутых DEK
};
use crate::meta::{read_meta, MetaHeader};

use super::{DATA_SEG_EXT, DATA_SEG_PREFIX, SEGMENT_SIZE};

/// Низкоуровневый менеджер страниц.
pub struct Pager {
    pub root: PathBuf,
    pub meta: MetaHeader,
    // Управляет fsync данных сегментов при записи страницы (write/commit).
    pub(crate) data_fsync: bool,

    // ----- TDE (prep) -----
    pub(crate) tde_enabled: bool,
    pub(crate) tde_kid: Option<String>,
    pub(crate) tde_key: Option<[u8; 32]>,

    // ----- OVF threshold (prep) -----
    pub(crate) ovf_threshold_bytes: Option<usize>,

    // ----- Stable DbId -----
    pub(crate) db_id: u64,
}

impl Pager {
    /// Открыть pager по meta v4.
    pub fn open(root: &Path) -> Result<Self> {
        let m = read_meta(root)?;
        if m.version != 4 {
            return Err(anyhow!(
                "meta version {} unsupported by 2.0 pager",
                m.version
            ));
        }
        let db_id = compute_db_id(root);
        Ok(Self {
            root: root.to_path_buf(),
            meta: m,
            data_fsync: true,
            tde_enabled: false,
            tde_kid: None,
            tde_key: None,
            ovf_threshold_bytes: None,
            db_id,
        })
    }

    /// Включить/выключить fsync данных при записях.
    pub fn set_data_fsync(&mut self, on: bool) { self.data_fsync = on; }
    pub fn data_fsync(&self) -> bool { self.data_fsync }

    // ----- TDE setters / getters -----

    pub fn set_tde_enabled(&mut self, on: bool) {
        self.tde_enabled = on;
        self.tde_key = None;
    }
    #[inline] pub fn tde_enabled(&self) -> bool { self.tde_enabled }
    #[inline] pub fn tde_key_loaded(&self) -> bool { self.tde_key.is_some() }

    pub fn set_tde_kid<S: Into<String>>(&mut self, kid: Option<S>) {
        self.tde_kid = kid.map(Into::into);
        self.tde_key = None;
    }
    #[inline] pub fn tde_kid(&self) -> Option<&str> { self.tde_kid.as_deref() }

    pub fn set_tde_config(&mut self, enabled: bool, kid: Option<String>) {
        self.tde_enabled = enabled;
        self.tde_kid = kid;
        self.tde_key = None;
    }

    /// Загрузить 32‑байтный ключ для AES‑GCM.
    /// Порядок:
    /// 1) Если есть keyring.bin и запись для KID — используем KMS (EnvKmsProvider) для unwrap.
    /// 2) Иначе — EnvKeyProvider (P1_TDE_KEY_HEX/BASE64).
    pub fn ensure_tde_key(&mut self) -> Result<()> {
        if !self.tde_enabled {
            return Ok(());
        }
        if self.tde_key.is_some() {
            return Ok(());
        }

        // Определим KID
        let kid_to_use: String = if let Some(k) = self.tde_kid.as_ref() {
            k.clone()
        } else if let Ok(j) = KeyJournal::open(&self.root) {
            if let Some(k) = j.last_kid()? { k } else {
                EnvKeyProvider::from_env().map(|p| p.default_kid().to_string()).unwrap_or_else(|_| "default".to_string())
            }
        } else {
            EnvKeyProvider::from_env().map(|p| p.default_kid().to_string()).unwrap_or_else(|_| "default".to_string())
        };

        // 1) Попробуем KeyRing + KMS (если есть запись для KID)
        if let Ok(kr) = KeyRing::open(&self.root) {
            if let Ok(Some(wrapped)) = kr.get(&kid_to_use) {
                let kms = EnvKmsProvider::from_env()
                    .context("EnvKmsProvider (set P1_KMS_KEK_HEX or P1_KMS_KEK_BASE64)")?;
                let (_kid, dek) = kms.unwrap(&wrapped)
                    .with_context(|| format!("KMS unwrap for KID '{}'", kid_to_use))?;
                if dek.len() != 32 {
                    return Err(anyhow!("unwrapped DEK must be 32 bytes, got {}", dek.len()));
                }
                let mut key = [0u8; 32];
                key.copy_from_slice(&dek[..32]);
                self.tde_key = Some(key);
                return Ok(());
            }
        }

        // 2) Fallback на EnvKeyProvider
        let provider = EnvKeyProvider::from_env()
            .context("EnvKeyProvider::from_env (set P1_TDE_KEY_HEX or P1_TDE_KEY_BASE64)")?;
        let km = provider.key(&kid_to_use)
            .with_context(|| format!("load TDE key for KID '{}'", kid_to_use))?;
        self.tde_key = Some(km.key);
        Ok(())
    }

    /// Получить ссылку на 32‑байтный ключ (после ensure_tde_key()).
    pub fn tde_key_bytes(&mut self) -> Result<&[u8; 32]> {
        if !self.tde_enabled {
            return Err(anyhow!("TDE is disabled"));
        }
        self.ensure_tde_key()?;
        self.tde_key.as_ref().ok_or_else(|| anyhow!("TDE key is not available"))
    }

    // ----- OVF threshold -----
    pub fn set_ovf_threshold_bytes(&mut self, thr: Option<usize>) { self.ovf_threshold_bytes = thr; }

    // ---------------- internal helpers ----------------

    /// Сколько страниц помещается в один сегмент при заданном page_size.
    pub(crate) fn pages_per_seg(&self) -> u64 {
        let ps = self.meta.page_size as u64; (SEGMENT_SIZE / ps).max(1)
    }

    /// Сопоставить page_id в (номер сегмента, смещение внутри сегмента).
    pub(crate) fn locate(&self, page_id: u64) -> (u64, u64) {
        let pps = self.pages_per_seg();
        let seg_no = (page_id / pps) + 1;
        let off_in_seg = (page_id % pps) * (self.meta.page_size as u64);
        (seg_no, off_in_seg)
    }

    /// Путь к файлу сегмента по его номеру.
    pub(crate) fn seg_path(&self, seg_no: u64) -> PathBuf {
        self.root.join(format!("{}{:06}.{}", DATA_SEG_PREFIX, seg_no, DATA_SEG_EXT))
    }

    /// Открыть сегмент на чтение/запись (create=true — создать, если отсутствует).
    pub(crate) fn open_seg_rw(&self, seg_no: u64, create: bool) -> Result<std::fs::File> {
        let path = self.seg_path(seg_no);
        let mut opts = OpenOptions::new(); opts.read(true).write(true);
        if create { opts.create(true); }
        opts.open(&path).with_context(|| format!("open segment {}", path.display()))
    }
}

/// Стабильный идентификатор БД (канонический путь + dev/ino на Unix).
fn compute_db_id(root: &Path) -> u64 {
    use std::hash::Hasher;
    let mut h = twox_hash::XxHash64::with_seed(0xD3B1_2A52_9F17_4B3C);
    let canon = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let s = canon.to_string_lossy(); h.write(s.as_bytes());
    #[cfg(unix)] {
        use std::os::unix::fs::MetadataExt;
        if let Ok(md) = std::fs::metadata(&canon) {
            h.write_u64(md.dev() as u64); h.write_u64(md.ino() as u64);
        }
    }
    h.finish()
}