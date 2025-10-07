//! C ABI (FFI) for QuiverDB.
//!
//! Scope (stable, minimal ABI):
//! - init/open/open_ro/close
//! - put/get/del
//! - scan_all / scan_prefix via callback
//! - version query
//!
//! Configuration (JSON for open_with_config/open_ro_with_config):
//! - wal_coalesce_ms: u64
//! - data_fsync: bool
//! - page_cache_pages: usize
//! - ovf_threshold_bytes: usize
//! - snap_persist: bool           (Phase 2; persisted snapshots)
//! - snap_dedup: bool             (Phase 2; SnapStore dedup)
//! - snapstore_dir: string        (Phase 2; custom SnapStore dir; absolute or relative to DB root)

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uchar, c_void};
use std::path::PathBuf;
use std::ptr;

use anyhow::{Context, Result};

use crate::{Db, Directory, init_db};
use crate::config::QuiverConfig;

// -------- last_error (TLS) --------

use std::cell::RefCell;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

fn set_last_error(e: anyhow::Error) {
    let s = e.to_string();
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(CString::new(s).unwrap_or_else(|_| CString::new("error").unwrap()));
    });
}

/// Returns a newly allocated copy of the last error string in the current thread (or NULL).
/// Free with qdb_free_string().
#[no_mangle]
pub extern "C" fn qdb_last_error_dup() -> *mut c_char {
    LAST_ERROR.with(|slot| {
        if let Some(s) = slot.borrow().as_ref() {
            // duplicate and transfer ownership to caller
            unsafe { libc_strdup(s.as_c_str().as_ptr()) }
        } else {
            ptr::null_mut()
        }
    })
}

/// Free a string returned by qdb_last_error_dup() or qdb_version_dup().
#[no_mangle]
pub extern "C" fn qdb_free_string(p: *mut c_char) {
    if !p.is_null() {
        unsafe {
            let _ = CString::from_raw(p);
        }
    }
}

// libc-like helpers implemented via Rust alloc (Windows safe)
unsafe fn libc_strdup(s: *const c_char) -> *mut c_char {
    if s.is_null() {
        return ptr::null_mut();
    }
    let len = libc_strlen(s);
    let size = len + 1;
    let buf = libc_malloc(size);
    if buf.is_null() {
        return ptr::null_mut();
    }
    ptr::copy_nonoverlapping(s as *const u8, buf as *mut u8, size);
    buf as *mut c_char
}

unsafe fn libc_strlen(s: *const c_char) -> usize {
    let mut p = s;
    let mut n = 0usize;
    while *p != 0 {
        p = p.add(1);
        n += 1;
    }
    n
}

unsafe fn libc_malloc(sz: usize) -> *mut u8 {
    let layout = std::alloc::Layout::from_size_align(sz, std::mem::align_of::<usize>()).unwrap();
    std::alloc::alloc(layout)
}

// -------- util: CStr -> Path --------

fn cstr_to_path(c: *const c_char) -> Result<PathBuf> {
    if c.is_null() {
        anyhow::bail!("null path");
    }
    let s = unsafe { CStr::from_ptr(c) }.to_str().context("path utf-8")?;
    Ok(PathBuf::from(s))
}

// -------- init/open/close --------

/// Initialize a new DB: creates meta/seg1/WAL/free and (optionally) a directory.
/// Returns 0 on success, -1 on error (see qdb_last_error_dup()).
#[no_mangle]
pub extern "C" fn qdb_init_db(
    path: *const c_char,
    page_size: u32,
    buckets: u32,
) -> c_int {
    let res = (|| -> Result<()> {
        let root = cstr_to_path(path)?;
        if !root.exists() || !root.join("meta").exists() {
            init_db(&root, page_size)?;
        }
        if !root.join("dir").exists() {
            Directory::create(&root, buckets)?;
        }
        Ok(())
    })();

    match res {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e);
            -1
        }
    }
}

/// Open writer (exclusive lock). Returns NULL on error.
#[no_mangle]
pub extern "C" fn qdb_open(path: *const c_char) -> *mut Handle {
    let res = (|| -> Result<Handle> {
        let root = cstr_to_path(path)?;
        let db = Db::open(&root)?;
        Ok(Handle { db, readonly: false })
    })();

    match res {
        Ok(h) => Box::into_raw(Box::new(h)),
        Err(e) => {
            set_last_error(e);
            ptr::null_mut()
        }
    }
}

/// Open reader (shared lock). Returns NULL on error.
#[no_mangle]
pub extern "C" fn qdb_open_ro(path: *const c_char) -> *mut Handle {
    let res = (|| -> Result<Handle> {
        let root = cstr_to_path(path)?;
        let db = Db::open_ro(&root)?;
        Ok(Handle { db, readonly: true })
    })();

    match res {
        Ok(h) => Box::into_raw(Box::new(h)),
        Err(e) => {
            set_last_error(e);
            ptr::null_mut()
        }
    }
}

// -------- NEW: open with JSON config --------

#[derive(serde::Deserialize, Default)]
struct FfiConfig {
    wal_coalesce_ms: Option<u64>,
    data_fsync: Option<bool>,
    page_cache_pages: Option<usize>,
    ovf_threshold_bytes: Option<usize>,
    // Phase 2:
    snap_persist: Option<bool>,
    snap_dedup: Option<bool>,
    snapstore_dir: Option<String>,
}

fn cfg_from_json(json_ptr: *const c_char) -> Result<QuiverConfig> {
    // Start from env to preserve current behavior, then override with JSON.
    let mut cfg = QuiverConfig::from_env();

    if json_ptr.is_null() {
        return Ok(cfg);
    }
    let s = unsafe { CStr::from_ptr(json_ptr) }
        .to_str()
        .context("config json utf-8")?;
    if s.trim().is_empty() {
        return Ok(cfg);
    }

    let parsed: FfiConfig = serde_json::from_str(s).context("parse config json")?;

    if let Some(ms) = parsed.wal_coalesce_ms { cfg.wal_coalesce_ms = ms; }
    if let Some(df) = parsed.data_fsync { cfg.data_fsync = df; }
    if let Some(pc) = parsed.page_cache_pages { cfg.page_cache_pages = pc; }
    if let Some(ovf) = parsed.ovf_threshold_bytes { cfg.ovf_threshold_bytes = Some(ovf); }

    // Phase 2 overrides
    if let Some(persist) = parsed.snap_persist { cfg.snap_persist = persist; }
    if let Some(dedup) = parsed.snap_dedup { cfg.snap_dedup = dedup; }
    if let Some(dir) = parsed.snapstore_dir {
        if !dir.trim().is_empty() {
            cfg.snapstore_dir = Some(dir);
        }
    }

    Ok(cfg)
}

/// Open writer with config (JSON over env). NULL on error.
#[no_mangle]
pub extern "C" fn qdb_open_with_config(path: *const c_char, cfg_json: *const c_char) -> *mut Handle {
    let res = (|| -> Result<Handle> {
        let root = cstr_to_path(path)?;
        let cfg = cfg_from_json(cfg_json)?;
        let db = Db::open_with_config(&root, cfg)?;
        Ok(Handle { db, readonly: false })
    })();

    match res {
        Ok(h) => Box::into_raw(Box::new(h)),
        Err(e) => { set_last_error(e); ptr::null_mut() }
    }
}

/// Open reader with config (JSON over env). NULL on error.
#[no_mangle]
pub extern "C" fn qdb_open_ro_with_config(path: *const c_char, cfg_json: *const c_char) -> *mut Handle {
    let res = (|| -> Result<Handle> {
        let root = cstr_to_path(path)?;
        let cfg = cfg_from_json(cfg_json)?;
        let db = Db::open_ro_with_config(&root, cfg)?;
        Ok(Handle { db, readonly: true })
    })();

    match res {
        Ok(h) => Box::into_raw(Box::new(h)),
        Err(e) => { set_last_error(e); ptr::null_mut() }
    }
}

/// Close a handle.
#[no_mangle]
pub extern "C" fn qdb_close(h: *mut Handle) {
    if !h.is_null() {
        unsafe { drop(Box::from_raw(h)); }
    }
}

// -------- put/get/del --------

struct Handle {
    db: Db,
    readonly: bool,
}

/// Insert/update (writer). 0 = OK, -1 = error, 1 = readonly.
#[no_mangle]
pub extern "C" fn qdb_put(
    h: *mut Handle,
    key_ptr: *const c_uchar,
    key_len: usize,
    val_ptr: *const c_uchar,
    val_len: usize,
) -> c_int {
    if h.is_null() { return -1; }
    let handle = unsafe { &mut *h };
    if handle.readonly { return 1; }
    let res = (|| -> Result<()> {
        if key_ptr.is_null() { anyhow::bail!("null key"); }
        if val_ptr.is_null() { anyhow::bail!("null value"); }
        let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
        let val = unsafe { std::slice::from_raw_parts(val_ptr, val_len) };
        handle.db.put(key, val)?;
        Ok(())
    })();
    match res { Ok(()) => 0, Err(e) => { set_last_error(e); -1 } }
}

/// Get. 0 => found (out_ptr/out_len set), 1 => not found, -1 => error.
/// Free buffer with qdb_free_buf().
#[no_mangle]
pub extern "C" fn qdb_get(
    h: *mut Handle,
    key_ptr: *const c_uchar,
    key_len: usize,
    out_ptr: *mut *mut c_uchar,
    out_len: *mut usize,
) -> c_int {
    if h.is_null() || key_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
        return -1;
    }
    let handle = unsafe { &mut *h };
    let res = (|| -> Result<c_int> {
        let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
        match handle.db.get(key)? {
            Some(v) => {
                let len = v.len();
                let mut boxed = v.into_boxed_slice();
                let ptr_u8 = boxed.as_mut_ptr();
                std::mem::forget(boxed); // give ownership to caller
                unsafe {
                    *out_ptr = ptr_u8;
                    *out_len = len;
                }
                Ok(0)
            }
            None => Ok(1),
        }
    })();
    match res { Ok(code) => code, Err(e) => { set_last_error(e); -1 } }
}

/// Delete. 0 => deleted, 1 => not found (or readonly), -1 => error.
#[no_mangle]
pub extern "C" fn qdb_del(
    h: *mut Handle,
    key_ptr: *const c_uchar,
    key_len: usize,
) -> c_int {
    if h.is_null() || key_ptr.is_null() { return -1; }
    let handle = unsafe { &mut *h };
    if handle.readonly { return 1; }
    let res = (|| -> Result<c_int> {
        let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
        let ok = handle.db.del(key)?;
        Ok(if ok { 0 } else { 1 })
    })();
    match res { Ok(code) => code, Err(e) => { set_last_error(e); -1 } }
}

/// Free a buffer returned by qdb_get().
#[no_mangle]
pub extern "C" fn qdb_free_buf(ptr_: *mut c_uchar, len: usize) {
    if ptr_.is_null() || len == 0 { return; }
    unsafe { let _ = Vec::from_raw_parts(ptr_, len, len); }
}

// -------- Streaming scans via callback --------

pub type qdb_scan_cb = extern "C" fn(
    key_ptr: *const c_uchar,
    key_len: usize,
    val_ptr: *const c_uchar,
    val_len: usize,
    user: *mut c_void,
);

/// Full scan. cb is called for each pair; data valid only during cb.
#[no_mangle]
pub extern "C" fn qdb_scan_all(
    h: *mut Handle,
    cb: Option<qdb_scan_cb>,
    user: *mut c_void,
) -> c_int {
    if h.is_null() { return -1; }
    let cb = match cb { Some(f) => f, None => return -1 };
    let handle = unsafe { &mut *h };
    let res = (|| -> Result<()> {
        let pairs = handle.db.scan_all()?;
        for (k, v) in pairs {
            cb(k.as_ptr(), k.len(), v.as_ptr(), v.len(), user);
        }
        Ok(())
    })();
    match res { Ok(()) => 0, Err(e) => { set_last_error(e); -1 } }
}

/// Prefix scan. cb is called for each pair; data valid only during cb.
#[no_mangle]
pub extern "C" fn qdb_scan_prefix(
    h: *mut Handle,
    prefix_ptr: *const c_uchar,
    prefix_len: usize,
    cb: Option<qdb_scan_cb>,
    user: *mut c_void,
) -> c_int {
    if h.is_null() || prefix_ptr.is_null() { return -1; }
    let cb = match cb { Some(f) => f, None => return -1 };
    let handle = unsafe { &mut *h };
    let res = (|| -> Result<()> {
        let pref = unsafe { std::slice::from_raw_parts(prefix_ptr, prefix_len) };
        let pairs = handle.db.scan_prefix(pref)?;
        for (k, v) in pairs {
            cb(k.as_ptr(), k.len(), v.as_ptr(), v.len(), user);
        }
        Ok(())
    })();
    match res { Ok(()) => 0, Err(e) => { set_last_error(e); -1 } }
}

// -------- Version --------

/// Return crate version string (free with qdb_free_string).
#[no_mangle]
pub extern "C" fn qdb_version_dup() -> *mut c_char {
    CString::new(env!("CARGO_PKG_VERSION")).unwrap().into_raw()
}