//! Lance FFI Bridge (Phase 3.1 - C++-driven Data Writing)
//!
//! This minimal FFI provides opaque pointer management for the Lance dataset writer.
//! The actual data writing is handled on the C++ side, which writes Lance format
//! directly to disk (metadata + parquet files).
//!
//! This approach avoids Rust dependency conflicts while maintaining FFI safety.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Opaque handle to a Lance writer
/// Stores dataset metadata; actual writing happens on C++ side
pub struct LanceWriterHandle {
    uri: String,
    batch_count: usize,
    row_count: usize,
    closed: bool,
}

impl LanceWriterHandle {
    fn new(uri: String) -> Self {
        LanceWriterHandle {
            uri,
            batch_count: 0,
            row_count: 0,
            closed: false,
        }
    }
}

/// Create a new Lance writer for writing to the specified URI.
///
/// # Arguments
/// * `uri_ptr` - C-string path to write to (e.g., "/tmp/dataset.lance")
/// * `arrow_schema_ptr` - Pointer to Arrow C Data Interface FFI_ArrowSchema struct (optional)
///
/// # Returns
/// Opaque pointer to LanceWriterHandle, or null on error
///
/// # Safety
/// The caller must:
/// - Ensure uri_ptr is a valid null-terminated C string
/// - Call lance_writer_destroy() on the returned pointer when done
#[no_mangle]
pub extern "C" fn lance_writer_create(
    uri_ptr: *const c_char,
    _arrow_schema_ptr: *const c_void,
) -> *mut LanceWriterHandle {
    catch_unwind(AssertUnwindSafe(|| {
        if uri_ptr.is_null() {
            eprintln!("Lance FFI Error: uri_ptr is null");
            return std::ptr::null_mut();
        }

        let uri = match unsafe { CStr::from_ptr(uri_ptr) }.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => {
                eprintln!("Lance FFI Error: uri_ptr is not valid UTF-8");
                return std::ptr::null_mut();
            }
        };

        let handle = LanceWriterHandle::new(uri.clone());
        eprintln!("Lance FFI: Writer created for URI: {}", uri);
        Box::into_raw(Box::new(handle))
    }))
    .unwrap_or_else(|_| {
        eprintln!("Lance FFI Error: Panic in lance_writer_create");
        std::ptr::null_mut()
    })
}

/// Write a batch of records to the Lance dataset.
///
/// # Arguments
/// * `writer_ptr` - Pointer to LanceWriterHandle from lance_writer_create()
/// * `arrow_array_ptr` - Pointer to Arrow C Data Interface FFI_ArrowArray struct
/// * `arrow_schema_ptr` - Pointer to Arrow C Data Interface FFI_ArrowSchema struct
///
/// # Returns
/// 0 on success, non-zero error code on failure
///
/// # Safety
/// The caller must:
/// - Ensure writer_ptr is valid and not null
/// - Not call this after lance_writer_close() has been called
#[no_mangle]
pub extern "C" fn lance_writer_write_batch(
    writer_ptr: *mut LanceWriterHandle,
    _arrow_array_ptr: *const c_void,
    _arrow_schema_ptr: *const c_void,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() {
            eprintln!("Lance FFI Error: writer_ptr is null");
            return 1;
        }

        let writer = unsafe { &mut *writer_ptr };

        if writer.closed {
            eprintln!("Lance FFI Error: Writer is already closed");
            return 2;
        }

        writer.batch_count += 1;

        // Note: Data writing is handled on the C++ side
        // This just tracks batch count for logging
        if writer.batch_count % 100 == 0 || writer.batch_count <= 3 {
            eprintln!("Lance FFI: Received batch {}", writer.batch_count);
        }

        0 // Success
    }))
    .unwrap_or_else(|_| {
        eprintln!("Lance FFI Error: Panic in lance_writer_write_batch");
        7
    })
}

/// Finalize and close the Lance writer.
///
/// Commits any pending writes. No further writes are allowed after calling this.
///
/// # Arguments
/// * `writer_ptr` - Pointer to LanceWriterHandle from lance_writer_create()
///
/// # Returns
/// 0 on success, non-zero error code on failure
///
/// # Safety
/// The caller must:
/// - Ensure writer_ptr is valid and not null
/// - Not call this multiple times on the same pointer
/// - Call lance_writer_destroy() after this function returns
#[no_mangle]
pub extern "C" fn lance_writer_close(writer_ptr: *mut LanceWriterHandle) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() {
            eprintln!("Lance FFI Error: writer_ptr is null");
            return 1;
        }

        let writer = unsafe { &mut *writer_ptr };

        if writer.closed {
            eprintln!("Lance FFI Error: Writer is already closed");
            return 2;
        }

        writer.closed = true;

        eprintln!(
            "Lance FFI: Closed writer for URI: {} ({} batches, {} rows)",
            writer.uri, writer.batch_count, writer.row_count
        );

        0 // Success
    }))
    .unwrap_or_else(|_| {
        eprintln!("Lance FFI Error: Panic in lance_writer_close");
        3
    })
}

/// Destroy and deallocate the Lance writer.
///
/// Frees all resources associated with the writer.
/// Do not use the writer_ptr after calling this function.
///
/// # Arguments
/// * `writer_ptr` - Pointer to LanceWriterHandle from lance_writer_create()
///
/// # Safety
/// The caller must:
/// - Ensure writer_ptr is a valid pointer returned from lance_writer_create()
/// - Not call this multiple times on the same pointer
/// - Not use the writer_ptr after calling this function
#[no_mangle]
pub extern "C" fn lance_writer_destroy(writer_ptr: *mut LanceWriterHandle) {
    catch_unwind(AssertUnwindSafe(|| {
        if !writer_ptr.is_null() {
            unsafe {
                let _ = Box::from_raw(writer_ptr);
            }
        }
    }))
    .unwrap_or_else(|_| {
        eprintln!("Lance FFI Error: Panic in lance_writer_destroy");
    })
}
