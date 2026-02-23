//! Lance FFI Bridge (Phase 2 - Native Lance Writing via Arrow FFI)
//! FFI Bridge for Arrow -> Lance

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow::ffi::{FFI_ArrowSchema, FFI_ArrowArray};
use arrow::ffi_stream::{FFI_ArrowArrayStream, ArrowArrayStreamReader};
use arrow::record_batch::{RecordBatch, RecordBatchReader};
use arrow::array::RecordBatchIterator;
use arrow::datatypes::{Schema, DataType, Field};
use arrow::error::ArrowError;
use tokio::runtime::Runtime;

// Lance Dependencies
use lance::dataset::{WriteParams, WriteMode, CommitBuilder};
use libc;

fn apply_compression_metadata(schema: &Schema) -> Schema {
    let fields: Vec<Field> = schema.fields().iter().map(|field| {
        let mut metadata = field.metadata().clone();

        // Skip compression hints for dictionary fields — Lance handles them natively
        // via DictionaryDataBlock where compute_stat() is a no-op (zero HLL overhead)
        if !matches!(field.data_type(), DataType::Dictionary(_, _)) {
            // Use lz4 for fast compression (quick and effective)
            metadata.insert("lance-encoding:compression".to_string(), "lz4".to_string());

            // Enable Byte Stream Split for better float compression
            match field.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    metadata.insert("lance-encoding:bss".to_string(), "auto".to_string());
                },
                _ => {}
            }
        }

        field.as_ref().clone().with_metadata(metadata)
    }).collect();

    Schema::new(fields).with_metadata(schema.metadata().clone())
}

enum WriterBackend {
    Buffered {
        batches: Vec<RecordBatch>,
        dataset_initialized: bool,
        fragment_count: usize,
        pending_row_count: usize,
    },
    Streaming {
        // Background task handle (stream-based)
        task: Option<tokio::task::JoinHandle<Result<(), lance::Error>>>,
    },
}

pub struct LanceWriterHandle {
    uri: String,
    schema: Option<arrow::datatypes::Schema>,
    /// Cached schema with compression metadata (computed once on first batch)
    compressed_schema: Option<Arc<Schema>>,
    batch_count: usize,
    row_count: usize,
    closed: bool,
    runtime: Runtime,
    backend: WriterBackend,
    write_params: WriteParamsConfig,
}

const FLUSH_BATCH_THRESHOLD: usize = 200;
const FLUSH_ROW_THRESHOLD: usize = 1_000_000;

#[derive(Debug, Clone, Copy)]
struct WriteParamsConfig {
    max_rows_per_file: usize,
    max_rows_per_group: usize,
    max_bytes_per_file: usize,
    skip_auto_cleanup: bool,
}

impl Default for WriteParamsConfig {
    fn default() -> Self {
        Self {
            max_rows_per_file: 50_000_000,
            max_rows_per_group: 8192,
            max_bytes_per_file: 0,
            skip_auto_cleanup: false,
        }
    }
}

impl LanceWriterHandle {
    fn new(uri: String, use_streaming: bool) -> Result<Self, String> {
        // Buffered path: all work happens synchronously inside block_on() calls.
        // A single-threaded executor is sufficient and avoids thread pool overhead.
        //
        // Streaming path: exactly one background task runs the Lance consumer.
        // More than 1 worker thread adds unnecessary context-switch overhead and
        // cross-core cache coherency cost without any parallelism benefit.
        let runtime = if use_streaming {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .map_err(|e| format!("Failed to create Tokio runtime: {}", e))?
        } else {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("Failed to create Tokio runtime: {}", e))?
        };

        let backend = if use_streaming {
            WriterBackend::Streaming {
                task: None, // Initialized when stream is provided
            }
        } else {
            WriterBackend::Buffered {
                batches: Vec::new(),
                dataset_initialized: false,
                fragment_count: 0,
                pending_row_count: 0,
            }
        };

        Ok(LanceWriterHandle {
            uri,
            schema: None,
            compressed_schema: None,
            batch_count: 0,
            row_count: 0,
            closed: false,
            runtime,
            backend,
            write_params: WriteParamsConfig::default(),
        })
    }

    fn import_ffi_batch(arrow_array_ptr: *mut FFI_ArrowArray, arrow_schema_ptr: *mut FFI_ArrowSchema) -> Result<RecordBatch, String> {
        unsafe {
            // TAKING OWNERSHIP: We convert raw pointers to unsafe FFI structs.
            // These structs implement Drop and will automatically call the C-side release() callback
            // when they go out of scope, decrementing refcounts on the buffers.
            let ffi_array = FFI_ArrowArray::from_raw(arrow_array_ptr);
            let ffi_schema = FFI_ArrowSchema::from_raw(arrow_schema_ptr);

            // Import using Arrow's official Zero-Copy FFI integration
            // This verifies the schema and array consistency and creates Arrow Arrays restricted to the FFI buffers.
            let array_data = arrow::ffi::from_ffi(ffi_array, &ffi_schema).map_err(|e| e.to_string())?;
            
            // RecordBatches are exported as a single StructArray from C++
            let struct_array = arrow::array::StructArray::from(array_data);
            
            // Convert back to RecordBatch (Zero-Copy)
            Ok(RecordBatch::from(&struct_array))
        }
    }

    fn flush_batches(&mut self) -> Result<(), String> {
        let config = self.write_params;
        match &mut self.backend {
            WriterBackend::Buffered { batches, dataset_initialized, fragment_count, pending_row_count } => {
                if batches.is_empty() { return Ok(()); }
                let uri = self.uri.clone();
                let drain_batches = std::mem::take(batches);
                let flush_batch_count = drain_batches.len();
                let flush_row_count = *pending_row_count;
                let schema_ref = drain_batches[0].schema();
                
                let mode = if *dataset_initialized { WriteMode::Append } else { WriteMode::Overwrite };
                let write_params = build_write_params_from(config, mode);

                let result = self.runtime.block_on(async {
                    let batch_iter = RecordBatchIterator::new(drain_batches.into_iter().map(Ok), schema_ref);
                    lance::Dataset::write(batch_iter, &uri, Some(write_params)).await
                });

                match result {
                    Ok(_) => {
                        *dataset_initialized = true;
                        *pending_row_count = 0;
                        *fragment_count += 1;
                        eprintln!("Lance FFI: Wrote {} buffered batches ({} rows)", flush_batch_count, flush_row_count);
                        Ok(())
                    }
                    Err(e) => Err(format!("Failed to write: {}", e)),
                }
            },
            WriterBackend::Streaming { .. } => Ok(()),
        }
    }

    fn send_batch(&mut self, _batch: RecordBatch) -> Result<(), String> {
        match &mut self.backend {
            WriterBackend::Streaming { .. } => Err("Streaming backend expects ArrowArrayStream".to_string()),
            _ => Ok(())
        }
    }

    fn start_stream(&mut self, stream_ptr: *mut FFI_ArrowArrayStream) -> Result<(), String> {
        let config = self.write_params;
        match &mut self.backend {
            WriterBackend::Streaming { task } => {
                if task.is_some() {
                    return Err("Streaming task already started".to_string());
                }

                if stream_ptr.is_null() {
                    return Err("Null ArrowArrayStream".to_string());
                }

                let result = unsafe { ArrowArrayStreamReader::from_raw(stream_ptr) };
                unsafe { libc::free(stream_ptr as *mut c_void) };
                let reader = result.map_err(|e| format!("Failed to import ArrowArrayStream: {}", e))?;

                let compressed_schema = Arc::new(apply_compression_metadata(reader.schema().as_ref()));
                let compression_reader = CompressionReader::new(reader, compressed_schema);
                let source: Box<dyn RecordBatchReader + Send> = Box::new(compression_reader);
                let uri_clone = self.uri.clone();
                let write_params = build_write_params_from(config, WriteMode::Overwrite);

                eprintln!("Lance FFI: Starting streaming background task with Arrow C Stream...");

                let task_handle = self.runtime.spawn(async move {
                    let transaction = lance::dataset::InsertBuilder::new(&uri_clone)
                        .with_params(&write_params)
                        .execute_uncommitted_stream(source)
                        .await?;

                    let mut commit_builder = CommitBuilder::new(&uri_clone)
                        .use_stable_row_ids(write_params.enable_stable_row_ids)
                        .enable_v2_manifest_paths(write_params.enable_v2_manifest_paths)
                        .with_skip_auto_cleanup(write_params.skip_auto_cleanup);

                    if let Some(version) = write_params.data_storage_version {
                        commit_builder = commit_builder.with_storage_format(version);
                    }
                    if let Some(handler) = write_params.commit_handler.clone() {
                        commit_builder = commit_builder.with_commit_handler(handler);
                    }
                    if let Some(store_params) = write_params.store_params.clone() {
                        commit_builder = commit_builder.with_store_params(store_params);
                    }
                    if let Some(session) = write_params.session.clone() {
                        commit_builder = commit_builder.with_session(session);
                    }

                    commit_builder.execute(transaction).await.map(|_| ())
                });

                *task = Some(task_handle);
                Ok(())
            },
            _ => Err("Start stream only valid for streaming backend".to_string()),
        }
    }
}

fn build_write_params_from(config: WriteParamsConfig, mode: WriteMode) -> WriteParams {
    let mut params = WriteParams {
        mode,
        ..Default::default()
    };
    if config.max_rows_per_file > 0 {
        params.max_rows_per_file = config.max_rows_per_file;
    }
    if config.max_rows_per_group > 0 {
        params.max_rows_per_group = config.max_rows_per_group;
    }
    if config.max_bytes_per_file > 0 {
        params.max_bytes_per_file = config.max_bytes_per_file;
    }
    params.skip_auto_cleanup = config.skip_auto_cleanup;
    params
}

struct CompressionReader {
    inner: ArrowArrayStreamReader,
    schema: Arc<Schema>,
}

impl CompressionReader {
    fn new(inner: ArrowArrayStreamReader, schema: Arc<Schema>) -> Self {
        Self { inner, schema }
    }
}

impl RecordBatchReader for CompressionReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

impl Iterator for CompressionReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|res| {
            res.and_then(|batch| {
                RecordBatch::try_new(self.schema.clone(), batch.columns().to_vec())
            })
        })
    }
}

// C Interface Exports

#[no_mangle]
pub extern "C" fn lance_writer_create(uri_ptr: *const c_char, _arrow_schema_ptr: *const c_void, use_streaming: c_int) -> *mut LanceWriterHandle {
    catch_unwind(AssertUnwindSafe(|| {
        if uri_ptr.is_null() { return std::ptr::null_mut(); }
        let uri = match unsafe { CStr::from_ptr(uri_ptr) }.to_str() {
            Ok(s) => s.to_string(), Err(_) => return std::ptr::null_mut(),
        };
        match LanceWriterHandle::new(uri, use_streaming != 0) {
            Ok(handle) => Box::into_raw(Box::new(handle)),
            Err(e) => { eprintln!("Lance FFI Create Error: {}", e); std::ptr::null_mut() }
        }
    })).unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "C" fn lance_writer_write_batch(writer_ptr: *mut LanceWriterHandle, arrow_array_ptr: *const c_void, arrow_schema_ptr: *const c_void) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() || arrow_array_ptr.is_null() || arrow_schema_ptr.is_null() { return 3; }
        let writer = unsafe { &mut *writer_ptr };
        if writer.closed { return 2; }

        let raw_batch = match LanceWriterHandle::import_ffi_batch(arrow_array_ptr as *mut _, arrow_schema_ptr as *mut _) {
            Ok(b) => b, Err(e) => { eprintln!("FFI Import Error: {}", e); return 4; }
        };

        // Apply compression metadata (cached - computed once on first batch)
        let compressed_schema = if let Some(ref cached) = writer.compressed_schema {
            cached.clone()
        } else {
            let schema = Arc::new(apply_compression_metadata(raw_batch.schema().as_ref()));
            writer.compressed_schema = Some(schema.clone());
            schema
        };

        // Zero-copy schema replacement (buffers are shared)
        let columns = raw_batch.columns().to_vec();
        let record_batch = match RecordBatch::try_new(compressed_schema, columns) {
             Ok(b) => b,
             Err(e) => { eprintln!("Compression schema update error: {}", e); return 4; }
        };

        if writer.schema.is_none() { writer.schema = Some(record_batch.schema().as_ref().clone()); }
        writer.row_count += record_batch.num_rows();
        writer.batch_count += 1;

        match &mut writer.backend {
            WriterBackend::Buffered { batches, pending_row_count, .. } => {
                *pending_row_count += record_batch.num_rows();
                batches.push(record_batch);
                if batches.len() >= FLUSH_BATCH_THRESHOLD || *pending_row_count >= FLUSH_ROW_THRESHOLD {
                    if let Err(e) = writer.flush_batches() { eprintln!("Flush Error: {}", e); return 5; }
                }
            },
            WriterBackend::Streaming { .. } => {
                if let Err(e) = writer.send_batch(record_batch) { eprintln!("Stream Send Error: {}", e); return 5; }
            }
        }
        if writer.batch_count % 100 == 0 || writer.batch_count <= 3 {
             eprintln!("Lance FFI: Batch {} ({} rows)", writer.batch_count, writer.row_count);
        }
        0
    })).unwrap_or(7)
}

#[no_mangle]
pub extern "C" fn lance_writer_start_stream(writer_ptr: *mut LanceWriterHandle, arrow_stream_ptr: *const c_void) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() || arrow_stream_ptr.is_null() { return 1; }
        let writer = unsafe { &mut *writer_ptr };
        if writer.closed { return 2; }

        let stream_ptr = arrow_stream_ptr as *mut FFI_ArrowArrayStream;
        match writer.start_stream(stream_ptr) {
            Ok(_) => 0,
            Err(e) => { eprintln!("Stream Start Error: {}", e); 5 }
        }
    })).unwrap_or(3)
}

#[no_mangle]
pub extern "C" fn lance_writer_set_write_params(
    writer_ptr: *mut LanceWriterHandle,
    max_rows_per_file: i64,
    max_rows_per_group: i64,
    max_bytes_per_file: i64,
    skip_auto_cleanup: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() { return 1; }
        let writer = unsafe { &mut *writer_ptr };
        if writer.closed { return 2; }

        if max_rows_per_file > 0 {
            writer.write_params.max_rows_per_file = max_rows_per_file as usize;
        }
        if max_rows_per_group > 0 {
            writer.write_params.max_rows_per_group = max_rows_per_group as usize;
        }
        if max_bytes_per_file > 0 {
            writer.write_params.max_bytes_per_file = max_bytes_per_file as usize;
        }
        writer.write_params.skip_auto_cleanup = skip_auto_cleanup != 0;
        0
    })).unwrap_or(3)
}

#[no_mangle]
pub extern "C" fn lance_writer_close(writer_ptr: *mut LanceWriterHandle) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() { return 1; }
        let writer = unsafe { &mut *writer_ptr };
        if writer.closed { return 2; }

        let res = match &mut writer.backend {
            WriterBackend::Buffered { .. } => writer.flush_batches(),
            WriterBackend::Streaming { task, .. } => {
                if let Some(handle) = task.take() {
                    match writer.runtime.block_on(handle) {
                         Ok(Ok(_)) => Ok(()),
                         Ok(Err(e)) => Err(format!("Lance Task Error: {}", e)),
                         Err(e) => Err(format!("Task Join Error: {}", e)),
                    }
                } else { Ok(()) }
            }
        };

        match res {
            Ok(_) => { eprintln!("Lance FFI: Closed {}", writer.uri); writer.closed = true; 0 },
            Err(e) => { eprintln!("Close Error: {}", e); 5 }
        }
    })).unwrap_or(3)
}

#[no_mangle]
pub extern "C" fn lance_writer_destroy(writer_ptr: *mut LanceWriterHandle) {
    if !writer_ptr.is_null() { unsafe { let _ = Box::from_raw(writer_ptr); } }
}
