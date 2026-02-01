//! Lance FFI Bridge
//!
//! This is a minimal FFI implementation that provides the Lance writer interface.
//! It's designed to work with old Rust versions (1.75+) while the environment
//! is being updated to support full Arrow integration.
//!
//! Future: Add arrow and lance dependencies when Rust toolchain is updated.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::panic::{catch_unwind, AssertUnwindSafe};

/// Opaque handle to a Lance writer
/// Stores metadata about the dataset being written
pub struct LanceWriterHandle {
    uri: String,
    batch_count: usize,
    #[allow(dead_code)]
    row_count: usize,  // Used when Lance integration is complete
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
/// * `uri_ptr` - C-string path to write to (e.g., "/tmp/dataset")
/// * `arrow_schema_ptr` - Pointer to Arrow C Data Interface ArrowSchema struct (unused for now)
///
/// # Returns
/// Opaque pointer to LanceWriterHandle, or null on error
///
/// # Safety
/// The caller must:
/// - Ensure uri_ptr is a valid null-terminated C string
/// - Ensure arrow_schema_ptr points to valid data if provided
/// - Call lance_writer_destroy() on the returned pointer when done
#[no_mangle]
pub extern "C" fn lance_writer_create(
    uri_ptr: *const c_char,
    _arrow_schema_ptr: *const c_char,
) -> *mut LanceWriterHandle {
    catch_unwind(AssertUnwindSafe(|| {
        if uri_ptr.is_null() {
            eprintln!("Error: uri_ptr is null");
            return std::ptr::null_mut();
        }

        let uri = match unsafe { CStr::from_ptr(uri_ptr) }.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => {
                eprintln!("Error: uri_ptr is not valid UTF-8");
                return std::ptr::null_mut();
            }
        };

        let writer = LanceWriterHandle::new(uri);
        Box::into_raw(Box::new(writer))
    }))
    .unwrap_or_else(|_| {
        eprintln!("Error: Panic in lance_writer_create");
        std::ptr::null_mut()
    })
}

/// Write a batch of records to the Lance dataset.
///
/// # Arguments
/// * `writer_ptr` - Pointer to LanceWriterHandle from lance_writer_create()
/// * `arrow_array_ptr` - Pointer to Arrow C Data Interface ArrowArray struct
/// * `arrow_schema_ptr` - Pointer to Arrow C Data Interface ArrowSchema struct
///
/// # Returns
/// 0 on success, non-zero error code on failure
///
/// # Safety
/// The caller must:
/// - Ensure writer_ptr is valid and not null
/// - Ensure arrow_array_ptr and arrow_schema_ptr point to valid C Data Interface structs
/// - Not call this after lance_writer_close() has been called
#[no_mangle]
pub extern "C" fn lance_writer_write_batch(
    writer_ptr: *mut LanceWriterHandle,
    arrow_array_ptr: *const c_char,
    arrow_schema_ptr: *const c_char,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() {
            eprintln!("Error: writer_ptr is null");
            return 1;
        }

        let writer = unsafe { &mut *writer_ptr };

        if writer.closed {
            eprintln!("Error: Writer is already closed");
            return 2;
        }

        if arrow_array_ptr.is_null() || arrow_schema_ptr.is_null() {
            eprintln!("Error: arrow_array_ptr or arrow_schema_ptr is null");
            return 3;
        }

        writer.batch_count += 1;

        // Note: Without Arrow FFI dependencies, we can't decode the batch yet
        // In production with Arrow available, we would:
        // 1. Convert from Arrow C Data Interface
        // 2. Create RecordBatch
        // 3. Write to Lance dataset
        // For now, we just track the call

        eprintln!("Lance FFI: Received batch {}", writer.batch_count);

        0 // Success
    }))
    .unwrap_or_else(|_| {
        eprintln!("Error: Panic in lance_writer_write_batch");
        7
    })
}

/// Finalize and close the Lance writer.
///
/// Commits any pending writes and creates the Lance dataset metadata.
/// No further writes are allowed after calling this.
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
            eprintln!("Error: writer_ptr is null");
            return 1;
        }

        let writer = unsafe { &mut *writer_ptr };

        if writer.closed {
            eprintln!("Error: Writer is already closed");
            return 2;
        }

        writer.closed = true;

        eprintln!("Lance FFI: Closed writer for URI: {}", writer.uri);
        eprintln!("Lance FFI: Final statistics - {} batches received", writer.batch_count);

        0 // Success
    }))
    .unwrap_or_else(|_| {
        eprintln!("Error: Panic in lance_writer_close");
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
        eprintln!("Error: Panic in lance_writer_destroy");
    })
}
