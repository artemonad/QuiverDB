# QuiverDB FFI (C ABI)

This document describes the C-compatible foreign function interface (FFI) for QuiverDB, enabling integration from C/C++ and most other languages that can call C functions (Go via cgo, Java via JNI/JNA, .NET via P/Invoke, Python via ctypes/cffi, etc.).

Scope (v1.1.5)
- Stable, minimal C ABI for initializing/opening a database and basic KV operations.
- No on-disk format changes (meta v3, page v2, WAL v1).
- Backward-compatible with the Rust API and CLI.

Non-goals (for this release)
- Streaming CDC (ship/apply) via FFI is not included yet.
- Full scan/iteration APIs are not part of this minimal FFI (can be added later).


## 1) Build and artifacts

Enable the FFI feature and build the dynamic library:

- Linux/macOS:
    - cargo build --release --features ffi
    - Artifacts: target/release/libQuiverDB.so (Linux), target/release/libQuiverDB.dylib (macOS)
- Windows (MSVC):
    - cargo build --release --features ffi
    - Artifact: target/release/QuiverDB.dll

Header generation (requires cbindgen):
- cargo install cbindgen
- cbindgen --crate QuiverDB --output quiverdb.h
- The cbindgen.toml in the repository configures the header (C language, include guards, stdint.h/stddef.h includes, etc.).

Linking (examples):
- gcc main.c -L target/release -lQuiverDB -o app (and set rpath or LD_LIBRARY_PATH/DYLD_LIBRARY_PATH)
- cl main.c QuiverDB.lib (or link dynamically with the dll nearby)


## 2) Conventions and memory model

- All functions are exported with C linkage: extern "C".
- Paths are UTF-8 strings (const char*) expected to be valid on the host OS.
- Return codes (c_int) follow this convention:
    - 0 = success
    - 1 = not found or read-only constraint (see per-function notes)
    - -1 = error (call qdb_last_error_dup() for a message)
- Error text:
    - Use qdb_last_error_dup() to fetch a newly allocated copy of the last error message in the calling thread; free it with qdb_free_string().
    - The error buffer is thread-local.
- Buffers returned by qdb_get():
    - The function allocates a new buffer and returns it via out_ptr/out_len. The caller must free it with qdb_free_buf().
- Thread safety:
    - A Handle should not be shared concurrently across threads without external synchronization. Operations are not re-entrant on the same Handle.
    - Process-level single-writer/multi-reader safety is enforced via file locks internally (writer uses an exclusive lock; readers use a shared lock).
- Lifetime of input buffers:
    - key/value pointers passed into functions must remain valid for the duration of the call.


## 3) Exported API

Headers (generated): quiverdb.h  
Library (dynamic): libQuiverDB.so|dylib / QuiverDB.dll

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
- typedef struct QdbHandle QdbHandle;  (opaque in C; in our impl it’s a Handle)
- QdbHandle* qdb_open(const char* path)
    - Opens a writer (exclusive lock). Returns NULL on error.
- QdbHandle* qdb_open_ro(const char* path)
    - Opens a reader (shared lock). Returns NULL on error.
- void qdb_close(QdbHandle* h)
    - Closes the handle and releases resources.

KV operations
- int qdb_put(QdbHandle* h, const uint8_t* key, size_t key_len, const uint8_t* val, size_t val_len)
    - Writer only. Returns:
        - 0 on success,
        - 1 if the handle is read-only,
        - -1 on error (check qdb_last_error_dup()).
- int qdb_get(QdbHandle* h, const uint8_t* key, size_t key_len, uint8_t** out_ptr, size_t* out_len)
    - Reader or writer. Returns:
        - 0 if found (out_ptr/out_len set to a newly allocated buffer; free with qdb_free_buf()),
        - 1 if not found,
        - -1 on error (check qdb_last_error_dup()).
- int qdb_del(QdbHandle* h, const uint8_t* key, size_t key_len)
    - Writer only. Returns:
        - 0 if a key was deleted,
        - 1 if the key was not found (or the handle is read-only),
        - -1 on error (check qdb_last_error_dup()).
- void qdb_free_buf(uint8_t* ptr, size_t len)
    - Frees a buffer previously returned by qdb_get().

Notes:
- qdb_put() stores large values out-of-page using the database’s overflow chains transparently. qdb_get() reconstructs values automatically.
- The database respects the same crash-safety guarantees as the Rust API (WAL + LSN-gating on v2 pages).
- Directory bucket count and page size are defined at initialization time (qdb_init_db) and persist in metadata.


## 4) Return codes summary

- qdb_init_db: 0 success, -1 error
- qdb_open / qdb_open_ro: NULL on error
- qdb_close: no return
- qdb_put: 0 success, 1 read-only handle, -1 error
- qdb_get: 0 found, 1 not found, -1 error
- qdb_del: 0 deleted, 1 not found (or read-only), -1 error
- qdb_last_error_dup: NULL if no error; otherwise, a newly allocated char*; free with qdb_free_string
- qdb_free_string / qdb_free_buf: no return


## 5) Minimal C usage example

```с
#include "quiverdb.h"
#include <stdio.h>
#include <stdint.h>
#include <string.h>

static void print_last_error(void) {
    char* msg = qdb_last_error_dup();
    if (msg) { fprintf(stderr, "error: %s\n", msg); qdb_free_string(msg); }
}

int main(void) {
    // Initialize a new DB and directory if needed
    if (qdb_init_db("./db", 4096, 128) != 0) {
        print_last_error();
        return 1;
    }

    // Open writer
    QdbHandle* w = qdb_open("./db");
    if (!w) { print_last_error(); return 1; }

    // Put a key
    const uint8_t* k = (const uint8_t*)"alpha";
    const uint8_t* v = (const uint8_t*)"1";
    if (qdb_put(w, k, 5, v, 1) != 0) { print_last_error(); qdb_close(w); return 1; }

    qdb_close(w);

    // Open reader
    QdbHandle* r = qdb_open_ro("./db");
    if (!r) { print_last_error(); return 1; }

    // Get
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
- gcc main.c -I. -L target/release -lQuiverDB -o app
- export LD_LIBRARY_PATH=target/release (Linux) or DYLD_LIBRARY_PATH=target/release (macOS)
- ./app

Build (Windows, MSVC):
- cl main.c QuiverDB.lib
- Ensure QuiverDB.dll is in PATH or alongside the executable.


## 6) Threading and locking model

- Writer handle (qdb_open) takes an exclusive file lock; only one writer per database path across processes.
- Reader handle (qdb_open_ro) takes a shared file lock; multiple readers can coexist; opening a writer while readers exist is subject to OS advisory lock behavior (as implemented in the Rust library).
- Do not call FFI functions concurrently on the same QdbHandle from multiple threads without external synchronization.
- Different handles can be used from different threads/processes as expected (subject to OS file-lock semantics).


## 7) Troubleshooting

- qdb_open/qdb_open_ro returns NULL:
    - Use qdb_last_error_dup() to fetch details (missing path, lock failure, etc.).
- qdb_put returns 1:
    - The handle is read-only; open a writer with qdb_open().
- qdb_get returns 1:
    - Key not found.
- Memory leaks in the host application:
    - Ensure qdb_free_buf() is called on every successful qdb_get().
    - Ensure qdb_free_string() is called on strings from qdb_last_error_dup().
- Non-UTF8 paths:
    - Paths must be UTF-8. Convert appropriately from your host language before calling.
- Crash safety:
    - The FFI layer uses the same WAL and LSN-gating mechanisms as the Rust API. Unclean shutdown of a writer triggers WAL replay on the next writer open.


## 8) Roadmap

Potential expansions of the FFI surface (non-breaking, additive):
- Streaming CDC (ship/apply) wrappers (stdin/out compatible) and ship filters.
- Scan/iteration APIs (cursor-based, prefix filters).
- Configuration injection (WAL coalescing, page cache size, overflow threshold).
- Subscription hooks (in-process callbacks via function pointers) with care for re-entrancy.


## 9) License and support

- License: MIT (see LICENSE).
- On-disk formats: frozen (meta v3, page v2, WAL v1).
- For issues/bugs/feedback: open an issue on the GitHub repository.

This FFI is designed to be a thin, stable layer over QuiverDB’s robust core. If you need additional functions or language-specific examples, please file a request — the surface can be extended in a backward-compatible fashion.