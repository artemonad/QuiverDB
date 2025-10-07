# QuiverDB FFI (C ABI) — v1.2.5

This document describes the C-compatible foreign function interface (FFI) for QuiverDB, enabling integration from C/C++ and most other languages that can call C functions (Go via cgo, Java via JNI/JNA, .NET via P/Invoke, Python via ctypes/cffi, etc.).

Status: v1.2.5 (on-disk formats frozen: meta v3, page v2, WAL v1)

Scope (stable, minimal ABI)
- Initialize/open a database and basic KV operations.
- Read-only and read-write handles with file-lock based single-writer/multi-reader safety.
- Streaming scans via callback (scan_all/scan_prefix).
- Configurable open via JSON overrides (over env), including Phase 2 options (persisted snapshots and snapstore).
- Simple error handling (thread-local last error string).
- Version query.

Non-goals (for this release)
- Streaming CDC (ship/apply) wrappers for FFI (can be added later).
- Full cursor/iterator API (can be added later).


## 1) Build and artifacts

Enable the FFI feature and build the dynamic library:

- Linux/macOS:
```bash
cargo build --release --features ffi
# Artifacts:
#   target/release/libQuiverDB.so   (Linux)
#   target/release/libQuiverDB.dylib (macOS)
```

- Windows (MSVC):
```bash
cargo build --release --features ffi
# Artifact:
#   target/release/QuiverDB.dll
```

Header generation (requires cbindgen):
```bash
cargo install cbindgen
cbindgen --crate QuiverDB --output quiverdb.h
# cbindgen.toml is provided in the repo
```

Linking (examples):
- Linux/macOS:
```bash
gcc main.c -I. -L target/release -lQuiverDB -o app
export LD_LIBRARY_PATH=target/release    # Linux
export DYLD_LIBRARY_PATH=target/release  # macOS
./app
```

- Windows (MSVC):
```bat
cl main.c QuiverDB.lib
# Ensure QuiverDB.dll is in PATH or next to your executable
```


## 2) Conventions and memory model

- All functions are exported with C linkage: extern "C".
- Paths are UTF-8 strings (const char*) valid on the host OS.
- Return codes (c_int) follow this convention (see per-function notes):
    - 0 = success (or found),
    - 1 = not found (or read-only constraint),
    - -1 = error (call qdb_last_error_dup()).
- Error text:
    - qdb_last_error_dup() returns a newly allocated copy of the last error message in the calling thread; free it with qdb_free_string().
    - The error buffer is thread-local.
- Buffers returned by qdb_get():
    - A new buffer is allocated; caller must free it with qdb_free_buf().
- Thread safety:
    - Do not call FFI functions concurrently on the same QdbHandle without external synchronization.
    - Different handles can be used from different threads/processes (subject to OS file-lock semantics).
- Lifetime of input buffers:
    - key/value pointers passed into functions must remain valid for the duration of the call.


## 3) Exported API

Header: quiverdb.h  
Library: libQuiverDB.so|dylib / QuiverDB.dll

Error utilities
- char* qdb_last_error_dup(void)
    - Returns a newly allocated copy of the last error message in the current thread, or NULL if none. Free with qdb_free_string().
- void qdb_free_string(char* p)
    - Frees a string previously returned by qdb_last_error_dup().

Initialization
- int qdb_init_db(const char* path, uint32_t page_size, uint32_t buckets)
    - Creates a new database at path if it does not exist (meta + first segment + WAL + free-list).
    - Creates a directory file with the given number of buckets if missing.
    - Returns 0 on success, -1 on error.

Handles (opaque)
- typedef struct QdbHandle QdbHandle;
- QdbHandle* qdb_open(const char* path)
    - Opens a writer (exclusive lock). Returns NULL on error.
- QdbHandle* qdb_open_ro(const char* path)
    - Opens a reader (shared lock). Returns NULL on error.
- void qdb_close(QdbHandle* h)
    - Closes the handle and releases resources.

Configurable open (JSON over env)
- QdbHandle* qdb_open_with_config(const char* path, const char* json)
- QdbHandle* qdb_open_ro_with_config(const char* path, const char* json)
    - The JSON overrides env-derived defaults (unset fields keep their env/default values).
    - Supported keys:
        - wal_coalesce_ms: u64
        - data_fsync: bool
        - page_cache_pages: usize
        - ovf_threshold_bytes: usize
        - snap_persist: bool            (Phase 2 — keep sidecar + registry)
        - snap_dedup: bool              (Phase 2 — enable SnapStore dedup + hashindex)
        - snapstore_dir: string         (Phase 2 — custom SnapStore directory; absolute or relative to DB root)
    - Example JSON:
        - {"wal_coalesce_ms":0,"data_fsync":true,"page_cache_pages":256,"snap_persist":true,"snap_dedup":true,"snapstore_dir":".snapstore_alt"}

KV operations
- int qdb_put(QdbHandle* h, const uint8_t* key, size_t key_len, const uint8_t* val, size_t val_len)
    - Writer only. Returns: 0 success, 1 read-only handle, -1 error.
- int qdb_get(QdbHandle* h, const uint8_t* key, size_t key_len, uint8_t** out_ptr, size_t* out_len)
    - Reader or writer. Returns: 0 found (set out_ptr/out_len), 1 not found, -1 error.
- int qdb_del(QdbHandle* h, const uint8_t* key, size_t key_len)
    - Writer only. Returns: 0 deleted, 1 not found (or read-only), -1 error.
- void qdb_free_buf(uint8_t* ptr, size_t len)
    - Frees a buffer previously returned by qdb_get().

Streaming scans (callback-based)
- typedef void (*qdb_scan_cb)(const uint8_t* key, size_t key_len, const uint8_t* val, size_t val_len, void* user);
- int qdb_scan_all(QdbHandle* h, qdb_scan_cb cb, void* user)
- int qdb_scan_prefix(QdbHandle* h, const uint8_t* prefix, size_t prefix_len, qdb_scan_cb cb, void* user)

Version
- char* qdb_version_dup(void)
    - Returns a newly allocated string containing the version (CARGO_PKG_VERSION). Free with qdb_free_string().


## 4) Return codes summary

- qdb_init_db: 0 success, -1 error
- qdb_open / qdb_open_ro / qdb_open_with_config / qdb_open_ro_with_config: NULL on error
- qdb_close: no return
- qdb_put: 0 success, 1 read-only handle, -1 error
- qdb_get: 0 found, 1 not found, -1 error
- qdb_del: 0 deleted, 1 not found (or read-only), -1 error
- qdb_scan_all / qdb_scan_prefix: 0 success, -1 error
- qdb_last_error_dup: NULL if no error; otherwise, a newly allocated char*; free with qdb_free_string
- qdb_free_string / qdb_free_buf: no return


## 5) Minimal C usage example

```c
#include "quiverdb.h"
#include <stdio.h>
#include <stdint.h>
#include <string.h>

static void print_last_error(void) {
    char* msg = qdb_last_error_dup();
    if (msg) { fprintf(stderr, "error: %s\n", msg); qdb_free_string(msg); }
}

static void scan_cb(const uint8_t* k, size_t klen, const uint8_t* v, size_t vlen, void* user) {
    (void)user;
    printf("key[%zu]: ", klen);
    fwrite(k, 1, klen, stdout);
    printf(" -> value[%zu]\n", vlen);
}

int main(void) {
    // Initialize a new DB and directory if needed
    if (qdb_init_db("./ffi_db", 4096, 128) != 0) {
        print_last_error();
        return 1;
    }

    // Open writer with JSON config: enable dedup/persist and set snapstore dir
    const char* json = "{\"wal_coalesce_ms\":0,\"data_fsync\":true,"
                       "\"snap_persist\":true,\"snap_dedup\":true,"
                       "\"snapstore_dir\":\".snapstore_alt\"}";
    QdbHandle* w = qdb_open_with_config("./ffi_db", json);
    if (!w) { print_last_error(); return 1; }

    // Put a key
    const uint8_t* k = (const uint8_t*)"alpha";
    const uint8_t* v = (const uint8_t*)"1";
    if (qdb_put(w, k, 5, v, 1) != 0) { print_last_error(); qdb_close(w); return 1; }

    // Scan all (callback)
    if (qdb_scan_all(w, scan_cb, NULL) != 0) { print_last_error(); }

    qdb_close(w);

    // Open reader and get
    QdbHandle* r = qdb_open_ro("./ffi_db");
    if (!r) { print_last_error(); return 1; }

    uint8_t* out = NULL; size_t out_len = 0;
    int rc = qdb_get(r, k, 5, &out, &out_len);
    if (rc == 0) {
        printf("FOUND: %.*s\n", (int)out_len, (const char*)out);
        qdb_free_buf(out, out_len);
    } else if (rc == 1) {
        printf("NOT FOUND\n");
    } else {
        print_last_error();
    }

    qdb_close(r);
    return 0;
}
```

Build (Linux/macOS):
```bash
gcc main.c -I. -L target/release -lQuiverDB -o app
export LD_LIBRARY_PATH=target/release
./app
```

Build (Windows, MSVC):
```bat
cl main.c QuiverDB.lib
# Ensure QuiverDB.dll is in PATH or alongside the executable
```


## 6) Threading and locking model

- Writer handle (qdb_open / qdb_open_with_config) takes an exclusive file lock; only one writer per database path across processes.
- Reader handle (qdb_open_ro / qdb_open_ro_with_config) takes a shared file lock; multiple readers can coexist; opening a writer while readers exist is subject to OS advisory lock behavior.
- Do not call FFI functions concurrently on the same QdbHandle from multiple threads without external synchronization.
- Different handles can be used from different threads/processes as expected (subject to OS file-lock semantics).


## 7) Configuration and environment

Order of precedence:
- JSON passed to qdb_open_with_config/qdb_open_ro_with_config overrides matching fields from env.
- For fields not set in JSON, QuiverConfig::from_env() is used.

Relevant env variables:
- Core:
    - P1_WAL_COALESCE_MS
    - P1_DATA_FSYNC=0|1
    - P1_PAGE_CACHE_PAGES
    - P1_OVF_THRESHOLD_BYTES
- Phase 2:
    - P1_SNAP_PERSIST=0|1 — keep sidecar, write .snapshots/registry.json
    - P1_SNAP_DEDUP=0|1 — enable SnapStore dedup (hashindex in sidecar)
    - P1_SNAPSTORE_DIR=path — custom SnapStore directory (absolute or relative to DB root)

Notes:
- On-disk formats remain frozen (meta v3, page v2, WAL v1).
- Phase 1 snapshots are in-process; Phase 2 adds persisted snapshots + SnapStore.


## 8) Troubleshooting

- qdb_open/qdb_open_ro returns NULL:
    - Call qdb_last_error_dup() for details (lock failure, path issues, etc.).
- qdb_put returns 1:
    - The handle is read-only; use qdb_open/qdb_open_with_config for a writer.
- qdb_get returns 1:
    - Key not found.
- Memory leaks in the host application:
    - Ensure qdb_free_buf() is called on every successful qdb_get().
    - Ensure qdb_free_string() is called on strings from qdb_last_error_dup().
- Snapshots:
    - Phase 1 snapshots are in-process and do not survive restarts.
    - Phase 2 features (persisted snapshots + SnapStore) require enabling snap_persist/snap_dedup via JSON or env.
- Crash safety:
    - WAL replay is executed by the next writer open if the previous shutdown was unclean.
- SnapStore:
    - When P1_SNAPSTORE_DIR is relative, it is resolved relative to the DB root.
    - Removing snapshots does not immediately shrink store.bin; run compact logic (library call) or use the CLI snapcompact command in the CLI build.