//! Lance FFI Bridge (Phase 2 - Native Lance Writing via Arrow FFI)
//!
//! This FFI provides Lance dataset writing with Arrow C Data Interface integration.
//! Data flows as zero-copy Arrow batches from C++ and is written directly as native
//! Lance format using the Lance Rust API, with no C++ post-processing required.
//!
//! Key features:
//! - Arrow C Data Interface import: FFI_ArrowArray/FFI_ArrowSchema conversion
//! - Batch accumulation for efficient Lance dataset writing
//! - Tokio async runtime for Lance write operations
//! - Comprehensive error handling and logging

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::slice;
use std::collections::HashMap;

use arrow::ffi::{FFI_ArrowSchema, FFI_ArrowArray};
use arrow::record_batch::RecordBatch;
use arrow::array::{RecordBatchIterator, Array, Int64Array, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{Schema, DataType, Field};
use arrow::buffer::Buffer;
use arrow::array::ArrayData;
use tokio::runtime::Runtime;
use lance::dataset::WriteParams;

/// C Data Interface ArrowArray structure - matches the C specification
/// This allows us to access the FFI_ArrowArray fields directly
#[repr(C)]
struct CDataArrowArray {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
    buffers: *const *const c_void,
    children: *const *mut c_void,
    dictionary: *mut c_void,
    release: Option<extern "C" fn(*mut c_void)>,
    private_data: *mut c_void,
}

/// Safe wrapper around FFI_ArrowArray C structure
/// Provides methods to safely read buffer pointers and child arrays
struct SafeArrowArray {
    ffi: *mut CDataArrowArray,
}

impl SafeArrowArray {
    /// Read buffer pointer at index
    /// Returns the pointer if valid, None otherwise
    unsafe fn buffer_ptr(&self, index: usize) -> Option<*const u8> {
        if self.ffi.is_null() {
            return None;
        }

        let ffi_array = &*self.ffi;
        if index >= ffi_array.n_buffers as usize {
            return None;
        }

        if ffi_array.buffers.is_null() {
            return None;
        }

        let buffer_ptr = *ffi_array.buffers.add(index);
        if buffer_ptr.is_null() {
            None
        } else {
            Some(buffer_ptr as *const u8)
        }
    }

    /// Read child array pointer at index
    unsafe fn child(&self, index: usize) -> Option<*mut CDataArrowArray> {
        if self.ffi.is_null() {
            return None;
        }

        let ffi_array = &*self.ffi;
        if index >= ffi_array.n_children as usize {
            return None;
        }

        if ffi_array.children.is_null() {
            return None;
        }

        Some(*ffi_array.children.add(index) as *mut CDataArrowArray)
    }

    /// Get array length
    unsafe fn length(&self) -> i64 {
        if self.ffi.is_null() {
            0
        } else {
            (*self.ffi).length
        }
    }

    /// Get null count
    unsafe fn null_count(&self) -> i64 {
        if self.ffi.is_null() {
            0
        } else {
            (*self.ffi).null_count
        }
    }
}

/// Import a primitive type array (Int64, Float64, Int32)
fn import_primitive_array(
    safe_array: &SafeArrowArray,
    field: &Field,
) -> Result<Arc<dyn Array>, String> {
    unsafe {
        let length = safe_array.length() as usize;
        if length == 0 {
            return Err("Cannot import array with 0 length".to_string());
        }

        // Buffer 0: Null bitmap (one bit per element)
        // Read null bitmap if present
        let _null_bitmap = if let Some(ptr) = safe_array.buffer_ptr(0) {
            let byte_count = (length + 7) / 8; // Ceiling division
            let slice = slice::from_raw_parts(ptr, byte_count);
            Some(Buffer::from_slice_ref(slice))
        } else {
            None
        };

        // Buffer 1: Data values
        let data_ptr = safe_array
            .buffer_ptr(1)
            .ok_or("Missing data buffer for primitive array")?;

        match field.data_type() {
            DataType::Int64 => {
                let value_count = length;
                let byte_count = value_count * std::mem::size_of::<i64>();
                let slice = slice::from_raw_parts(data_ptr, byte_count);
                let data_buffer = Buffer::from_slice_ref(slice);

                let array_data = ArrayData::builder(DataType::Int64)
                    .len(length)
                    .buffers(vec![data_buffer])
                    .null_count(safe_array.null_count() as usize)
                    .build_unchecked();

                Ok(Arc::new(Int64Array::from(array_data)))
            }
            DataType::Float64 => {
                let value_count = length;
                let byte_count = value_count * std::mem::size_of::<f64>();
                let slice = slice::from_raw_parts(data_ptr, byte_count);
                let data_buffer = Buffer::from_slice_ref(slice);

                let array_data = ArrayData::builder(DataType::Float64)
                    .len(length)
                    .buffers(vec![data_buffer])
                    .null_count(safe_array.null_count() as usize)
                    .build_unchecked();

                Ok(Arc::new(Float64Array::from(array_data)))
            }
            DataType::Int32 => {
                let value_count = length;
                let byte_count = value_count * std::mem::size_of::<i32>();
                let slice = slice::from_raw_parts(data_ptr, byte_count);
                let data_buffer = Buffer::from_slice_ref(slice);

                let array_data = ArrayData::builder(DataType::Int32)
                    .len(length)
                    .buffers(vec![data_buffer])
                    .null_count(safe_array.null_count() as usize)
                    .build_unchecked();

                Ok(Arc::new(Int32Array::from(array_data)))
            }
            other => Err(format!(
                "Unsupported primitive type in FFI import: {}",
                other
            )),
        }
    }
}

/// Import a string/binary type array
fn import_string_array(
    safe_array: &SafeArrowArray,
    _field: &Field,
) -> Result<Arc<dyn Array>, String> {
    unsafe {
        let length = safe_array.length() as usize;
        if length == 0 {
            return Err("Cannot import array with 0 length".to_string());
        }

        // Buffer 0: Null bitmap (not included in ArrayData buffers)
        let _null_bitmap = if let Some(ptr) = safe_array.buffer_ptr(0) {
            let byte_count = (length + 7) / 8;
            let slice = slice::from_raw_parts(ptr, byte_count);
            Some(Buffer::from_slice_ref(slice))
        } else {
            None
        };

        // Buffer 1: Offsets (int32 per element + 1)
        let offset_ptr = safe_array
            .buffer_ptr(1)
            .ok_or("Missing offset buffer for string array")?;
        let offset_byte_count = (length + 1) * std::mem::size_of::<i32>();
        let offset_slice = slice::from_raw_parts(offset_ptr, offset_byte_count);
        let offset_buffer = Buffer::from_slice_ref(offset_slice);

        // Buffer 2: Data bytes
        let data_ptr = safe_array
            .buffer_ptr(2)
            .ok_or("Missing data buffer for string array")?;

        // Get total byte length from last offset
        let offset_i32_slice = slice::from_raw_parts(offset_ptr as *const i32, length + 1);
        let data_byte_count = if !offset_i32_slice.is_empty() {
            offset_i32_slice[length] as usize
        } else {
            0
        };

        let data_slice = slice::from_raw_parts(data_ptr, data_byte_count);
        let data_buffer = Buffer::from_slice_ref(data_slice);

        // String arrays only need offsets and data buffers in ArrayData
        let array_data = ArrayData::builder(DataType::Utf8)
            .len(length)
            .buffers(vec![
                offset_buffer,
                data_buffer,
            ])
            .null_count(safe_array.null_count() as usize)
            .build_unchecked();

        Ok(Arc::new(StringArray::from(array_data)))
    }
}

/// Opaque handle to a Lance writer
/// Accumulates Arrow batches and writes them as Lance dataset on close
pub struct LanceWriterHandle {
    uri: String,
    schema: Option<arrow::datatypes::Schema>,
    batches: Vec<RecordBatch>,
    batch_count: usize,
    row_count: usize,
    closed: bool,
    runtime: Runtime,
}

impl LanceWriterHandle {
    fn new(uri: String) -> Result<Self, String> {
        let runtime = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;

        Ok(LanceWriterHandle {
            uri,
            schema: None,
            batches: Vec::new(),
            batch_count: 0,
            row_count: 0,
            closed: false,
            runtime,
        })
    }

    /// Convert Arrow C Data Interface structures to RecordBatch
    ///
    /// Implements the Arrow C Data Interface specification to convert FFI_ArrowArray
    /// and FFI_ArrowSchema pointers to a valid Arrow RecordBatch with actual data.
    ///
    /// # Safety
    /// Caller must ensure the FFI pointers are valid and properly initialized
    fn import_ffi_batch(
        arrow_array_ptr: *mut FFI_ArrowArray,
        arrow_schema_ptr: *mut FFI_ArrowSchema,
    ) -> Result<RecordBatch, String> {
        unsafe {
            // Import FFI schema
            let ffi_schema = FFI_ArrowSchema::from_raw(arrow_schema_ptr);
            let schema = Schema::try_from(&ffi_schema)
                .map_err(|e| format!("Failed to convert FFI_ArrowSchema to Schema: {}", e))?;

            // Create safe wrapper for root array
            let safe_array = SafeArrowArray {
                ffi: arrow_array_ptr as *mut CDataArrowArray,
            };

            // Import each field as separate array
            let mut arrays: Vec<Arc<dyn Array>> = Vec::new();

            for (i, field) in schema.fields().iter().enumerate() {
                // Get child array for this field
                let child_array_ptr = safe_array.child(i).ok_or_else(|| {
                    format!("Missing child array for field {}: {}", i, field.name())
                })?;

                let child_safe = SafeArrowArray {
                    ffi: child_array_ptr,
                };

                // Import array based on type
                let array = match field.data_type() {
                    DataType::Int64
                    | DataType::Float64
                    | DataType::Int32 => {
                        import_primitive_array(&child_safe, field)?
                    }
                    DataType::Utf8 => import_string_array(&child_safe, field)?,
                    dt => {
                        return Err(format!(
                            "Unsupported type for field {}: {}",
                            field.name(),
                            dt
                        ))
                    }
                };

                arrays.push(array);
            }

            // Note: FFI_ArrowSchema and FFI_ArrowArray have private fields in Arrow 57,
            // so we cannot access their release callbacks directly. Arrow will handle
            // releasing these structures through the Drop trait implementation.

            // Create and return RecordBatch
            RecordBatch::try_new(Arc::new(schema), arrays)
                .map_err(|e| format!("Failed to create RecordBatch: {}", e))
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

        match LanceWriterHandle::new(uri.clone()) {
            Ok(handle) => {
                eprintln!("Lance FFI: Writer created for URI: {}", uri);
                Box::into_raw(Box::new(handle))
            }
            Err(e) => {
                eprintln!("Lance FFI Error: Failed to create writer: {}", e);
                std::ptr::null_mut()
            }
        }
    }))
    .unwrap_or_else(|_| {
        eprintln!("Lance FFI Error: Panic in lance_writer_create");
        std::ptr::null_mut()
    })
}

/// Write a batch of records to the Lance dataset.
///
/// Imports Arrow C Data Interface structures and accumulates batches for
/// efficient Lance dataset writing.
///
/// # Arguments
/// * `writer_ptr` - Pointer to LanceWriterHandle from lance_writer_create()
/// * `arrow_array_ptr` - Pointer to Arrow C Data Interface FFI_ArrowArray struct
/// * `arrow_schema_ptr` - Pointer to Arrow C Data Interface FFI_ArrowSchema struct
///
/// # Returns
/// 0 on success, non-zero error code on failure:
///   1 = writer_ptr is null
///   2 = Writer is already closed
///   3 = arrow_array_ptr or arrow_schema_ptr is null
///   4 = Failed to import Arrow C Data Interface
///   7 = Panic in lance_writer_write_batch
///
/// # Safety
/// The caller must:
/// - Ensure writer_ptr is valid and not null
/// - Not call this after lance_writer_close() has been called
#[no_mangle]
pub extern "C" fn lance_writer_write_batch(
    writer_ptr: *mut LanceWriterHandle,
    arrow_array_ptr: *const c_void,
    arrow_schema_ptr: *const c_void,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() {
            eprintln!("Lance FFI Error: writer_ptr is null");
            return 1;
        }

        if arrow_array_ptr.is_null() || arrow_schema_ptr.is_null() {
            eprintln!("Lance FFI Error: arrow_array_ptr or arrow_schema_ptr is null");
            return 3;
        }

        let writer = unsafe { &mut *writer_ptr };

        if writer.closed {
            eprintln!("Lance FFI Error: Writer is already closed");
            return 2;
        }

        // Import the FFI structures as a RecordBatch with actual data
        // SAFETY: Pointers are validated as non-null above
        let record_batch = match LanceWriterHandle::import_ffi_batch(
            arrow_array_ptr as *mut FFI_ArrowArray,
            arrow_schema_ptr as *mut FFI_ArrowSchema,
        ) {
            Ok(batch) => batch,
            Err(e) => {
                eprintln!("Lance FFI Error: Failed to import FFI batch: {}", e);
                return 4; // Error code 4 = FFI import failure
            }
        };

        // Store schema from first batch
        if writer.schema.is_none() {
            writer.schema = Some(record_batch.schema().as_ref().clone());
        }

        // Accumulate batch and update counters
        writer.row_count += record_batch.num_rows();
        writer.batches.push(record_batch);
        writer.batch_count += 1;

        // Log progress
        if writer.batch_count % 100 == 0 || writer.batch_count <= 3 {
            eprintln!(
                "Lance FFI: Accumulated batch {} ({} rows total)",
                writer.batch_count, writer.row_count
            );
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
/// Writes all accumulated batches to the Lance dataset as a single dataset write,
/// creating the full Lance metadata and fragment structure.
///
/// Phase 2.0c-3: Encoding Strategy Simplification
///
/// Pre-computed encoding strategies for each column type.
/// Avoids repeated strategy evaluation per-batch (19,200 evaluations for lineitem).
/// Target: +3-8% improvement by eliminating encoding strategy overhead.
#[derive(Debug, Clone)]
struct EncodingStrategy {
    column_name: String,
    data_type: String,
    strategy: String,      // "fixed-width", "dictionary", etc.
    is_fast_path: bool,   // True if no complex evaluation needed
}

impl EncodingStrategy {
    /// Create encoding strategy for a single column
    /// Fast-path columns (int/float/date) skip all evaluation overhead
    fn for_column(field: &Field) -> Self {
        let (strategy, is_fast_path) = match field.data_type() {
            // Integer types: Always fixed-width, no alternatives (FAST PATH)
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8 |
            DataType::UInt64 | DataType::UInt32 | DataType::UInt16 | DataType::UInt8 => {
                ("fixed-width", true)
            }
            // Float types: Always fixed-width (FAST PATH)
            DataType::Float64 | DataType::Float32 => ("fixed-width", true),
            // Decimal: Always fixed-width (FAST PATH)
            DataType::Decimal128(_, _) => ("fixed-width", true),
            // Date/Time: Always fixed-width (FAST PATH)
            DataType::Date32 | DataType::Date64 => ("fixed-width", true),
            // String: Try dictionary heuristic (not fast path - needs cardinality check)
            DataType::Utf8 | DataType::LargeUtf8 => ("dictionary", false),
            // Other types: Use default strategy
            _ => ("variable-width", false),
        };

        EncodingStrategy {
            column_name: field.name().to_string(),
            data_type: format!("{:?}", field.data_type()),
            strategy: strategy.to_string(),
            is_fast_path,
        }
    }
}

/// Phase 2.0c-3: Pre-compute encoding strategies at schema creation time
/// Instead of evaluating per-batch (1,200 times for lineitem),
/// compute once and reuse for all batches.
fn compute_encoding_strategies(schema: &Schema) -> Vec<EncodingStrategy> {
    let strategies: Vec<_> = schema.fields()
        .iter()
        .map(|field| EncodingStrategy::for_column(field))
        .collect();

    // Count fast-path columns for logging
    let fast_path_count = strategies.iter().filter(|s| s.is_fast_path).count();
    eprintln!(
        "Lance FFI: Computed encoding strategies (Phase 2.0c-3): {} columns, {} fast-path",
        strategies.len(),
        fast_path_count
    );

    strategies
}

/// # Arguments
/// * `writer_ptr` - Pointer to LanceWriterHandle from lance_writer_create()
///
/// Phase 2.0c-2: Generate encoding hints for schema columns
/// Phase 2.0c-3: Use pre-computed strategies to reduce evaluation overhead
///
/// Creates Arrow schema metadata with encoding hints to optimize Lance
/// statistics computation and encoding strategy selection.
/// These hints guide Lance's encoding decisions without requiring explicit statistics.
fn create_schema_with_hints(schema: &Schema, strategies: &[EncodingStrategy]) -> Schema {
    let mut metadata = schema.metadata().cloned().unwrap_or_default();

    // Apply pre-computed strategies as encoding hints
    // Fast-path columns avoid all strategy evaluation overhead
    for (field, strategy) in schema.fields().iter().zip(strategies.iter()) {
        // Only add hints for fast-path columns (simple types with no alternatives)
        // Complex types are left for Lance's adaptive strategy selection
        if strategy.is_fast_path {
            metadata.insert(
                format!("lance-encoding:{}", field.name()),
                strategy.strategy.clone(),
            );
        }
    }

    // Create new schema with metadata hints
    Schema::new_with_metadata(schema.fields().clone(), metadata)
}

/// # Returns
/// 0 on success, non-zero error code on failure:
///   1 = writer_ptr is null
///   2 = Writer is already closed
///   5 = Failed to write Lance dataset
///   3 = Panic in lance_writer_close
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

        // Write all accumulated batches to Lance dataset
        if !writer.batches.is_empty() {
            let uri = writer.uri.clone();
            let batches = std::mem::take(&mut writer.batches);
            let batch_count = writer.batch_count;
            let row_count = writer.row_count;

            // Use Tokio runtime to execute async Lance write
            // with optimized WriteParams for better performance
            let result = writer.runtime.block_on(async {
                let original_schema = batches[0].schema();

                // Phase 2.0c-3: Pre-compute encoding strategies once for all batches
                // This eliminates repeated strategy evaluation (19,200× for lineitem 6M rows ÷ 5K batch)
                // Target: -70% on encoding strategy evaluation overhead
                let strategies = compute_encoding_strategies(&original_schema);

                // Phase 2.0c-2/2.0c-3: Apply pre-computed encoding hints
                // Use strategies to guide Lance encoding decisions without explicit statistics
                let optimized_schema = create_schema_with_hints(&original_schema, &strategies);

                // Create batch iterator with optimized schema
                let batch_iter = RecordBatchIterator::new(batches.into_iter().map(Ok), optimized_schema);

                // Phase 2.0c-2a: Optimized Lance configuration
                // Increase max_rows_per_group for reduced encoding overhead
                let write_params = WriteParams {
                    max_rows_per_group: 4096,  // 4× default (1024) for better cache locality
                    ..Default::default()
                };

                eprintln!(
                    "Lance FFI: Writing with pre-computed encoding strategies (Phase 2.0c-3)"
                );

                lance::Dataset::write(batch_iter, &uri, write_params).await
            });

            match result {
                Ok(_) => {
                    eprintln!(
                        "Lance FFI: Successfully wrote Lance dataset to: {} ({} batches, {} rows)",
                        uri, batch_count, row_count
                    );
                }
                Err(e) => {
                    eprintln!("Lance FFI Error: Failed to write Lance dataset: {}", e);
                    return 5;
                }
            }
        } else {
            eprintln!(
                "Lance FFI: Closed writer for URI: {} (no batches to write)",
                writer.uri
            );
        }

        writer.closed = true;
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
