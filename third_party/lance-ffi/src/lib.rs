//! Lance FFI Bridge (Phase 2 - Native Lance Writing via Arrow FFI)
//! FFI Bridge for Arrow -> Lance

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

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

// io_uring write path — compiled in when feature is enabled on Linux.
// Activated at runtime via lance_writer_enable_io_uring().
// The module uses Unix-only APIs (AsRawFd, MetadataExt, /sys/dev/block/…)
// so it must be guarded by both the feature flag and target_os.
#[cfg(all(feature = "io-uring", target_os = "linux"))]
mod io_uring_store;

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
    use_streaming: bool,
    backend: WriterBackend,
    write_params: WriteParamsConfig,
    runtime_config: RuntimeConfig,
    profile_config: ProfileConfig,
}

const FLUSH_BATCH_THRESHOLD: usize = 200;
const FLUSH_ROW_THRESHOLD: usize = 1_000_000;

#[derive(Debug, Clone, Copy)]
struct WriteParamsConfig {
    max_rows_per_file: usize,
    max_rows_per_group: usize,
    max_bytes_per_file: usize,
    skip_auto_cleanup: bool,
    use_io_uring: bool,
    scatter_gather_batches: usize,
    scatter_gather_queue_chunks: usize,
    buffered_flush_batch_threshold: usize,
    buffered_flush_row_threshold: usize,
}

#[derive(Debug, Clone, Copy)]
struct RuntimeConfig {
    /// Cap Tokio blocking pool size to avoid large stack reservations.
    max_blocking_threads: usize,
}

#[derive(Debug, Clone, Copy)]
struct ProfileConfig {
    enable_mem_profile: bool,
    report_every_batches: usize,
}

impl Default for ProfileConfig {
    fn default() -> Self {
        Self {
            enable_mem_profile: false,
            report_every_batches: 100,
        }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_blocking_threads: 8,
        }
    }
}

fn current_rss_kb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if !line.starts_with("VmRSS:") {
            continue;
        }
        let value = line.split_whitespace().nth(1)?;
        return value.parse::<u64>().ok();
    }
    None
}

fn log_mem_stage(profile: ProfileConfig, stage: &str, elapsed: Option<f64>) {
    if !profile.enable_mem_profile {
        return;
    }
    let rss = current_rss_kb().unwrap_or(0);
    if let Some(sec) = elapsed {
        eprintln!(
            "Lance FFI mem: stage={} rss_kb={} elapsed_s={:.6}",
            stage, rss, sec
        );
    } else {
        eprintln!("Lance FFI mem: stage={} rss_kb={}", stage, rss);
    }
}

impl Default for WriteParamsConfig {
    fn default() -> Self {
        Self {
            max_rows_per_file: 50_000_000,
            max_rows_per_group: 8192,
            max_bytes_per_file: 0,
            skip_auto_cleanup: false,
            use_io_uring: false,
            scatter_gather_batches: 1,
            scatter_gather_queue_chunks: 4,
            buffered_flush_batch_threshold: FLUSH_BATCH_THRESHOLD,
            buffered_flush_row_threshold: FLUSH_ROW_THRESHOLD,
        }
    }
}

impl LanceWriterHandle {
    fn build_runtime(use_streaming: bool, runtime_config: RuntimeConfig) -> Result<Runtime, String> {
        let max_blocking_threads = runtime_config.max_blocking_threads.max(1);
        let mut builder = if use_streaming {
            let mut b = tokio::runtime::Builder::new_multi_thread();
            b.worker_threads(1);
            b
        } else {
            tokio::runtime::Builder::new_current_thread()
        };

        builder
            .max_blocking_threads(max_blocking_threads)
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to create Tokio runtime: {}", e))
    }

    fn new(uri: String, use_streaming: bool) -> Result<Self, String> {
        let runtime_config = RuntimeConfig::default();
        let runtime = Self::build_runtime(use_streaming, runtime_config)?;

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
            use_streaming,
            backend,
            write_params: WriteParamsConfig::default(),
            runtime_config,
            profile_config: ProfileConfig::default(),
        })
    }

    fn set_runtime_config(&mut self, max_blocking_threads: usize) -> Result<(), String> {
        let WriterBackend::Streaming { task } = &self.backend else {
            return Ok(());
        };
        if task.is_some() {
            return Err("Cannot change runtime config after streaming task has started".to_string());
        }

        if max_blocking_threads > 0 {
            self.runtime_config.max_blocking_threads = max_blocking_threads;
        }
        self.runtime = Self::build_runtime(self.use_streaming, self.runtime_config)?;
        Ok(())
    }

    fn set_profile_config(&mut self, enable_mem_profile: bool, report_every_batches: usize) -> Result<(), String> {
        let WriterBackend::Streaming { task } = &self.backend else {
            self.profile_config.enable_mem_profile = enable_mem_profile;
            if report_every_batches > 0 {
                self.profile_config.report_every_batches = report_every_batches;
            }
            return Ok(());
        };
        if task.is_some() {
            return Err("Cannot change profile config after streaming task has started".to_string());
        }
        self.profile_config.enable_mem_profile = enable_mem_profile;
        if report_every_batches > 0 {
            self.profile_config.report_every_batches = report_every_batches;
        }
        Ok(())
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

                let profile = self.profile_config;
                let source: Box<dyn RecordBatchReader + Send> = if config.scatter_gather_batches > 1 {
                    Box::new(ScatterGatherReader::spawn(
                        reader,
                        profile,
                        config.scatter_gather_batches,
                        config.scatter_gather_queue_chunks,
                    )?)
                } else {
                    let compressed_schema = Arc::new(apply_compression_metadata(reader.schema().as_ref()));
                    let compression_reader = CompressionReader::new(reader, compressed_schema, profile);
                    Box::new(compression_reader)
                };
                let uri_clone = self.uri.clone();
                let write_params = build_write_params_from(config, WriteMode::Overwrite);

                eprintln!("Lance FFI: Starting streaming background task with Arrow C Stream...");
                eprintln!(
                    "Lance FFI: Tokio runtime mode=multi-thread(1 worker), max_blocking_threads={}",
                    self.runtime_config.max_blocking_threads
                );
                if self.profile_config.enable_mem_profile {
                    eprintln!(
                        "Lance FFI mem: enabled=1 report_every_batches={}",
                        self.profile_config.report_every_batches
                    );
                }
                eprintln!(
                    "Lance FFI: scatter/gather batches_per_chunk={}, queue_chunks={}",
                    config.scatter_gather_batches,
                    config.scatter_gather_queue_chunks
                );

                let task_handle = self.runtime.spawn(async move {
                    log_mem_stage(profile, "stream_task_start", None);
                    let stream_begin = Instant::now();
                    log_mem_stage(profile, "before_execute_uncommitted_stream", None);
                    let transaction = lance::dataset::InsertBuilder::new(&uri_clone)
                        .with_params(&write_params)
                        .execute_uncommitted_stream(source)
                        .await?;
                    log_mem_stage(
                        profile,
                        "after_execute_uncommitted_stream",
                        Some(stream_begin.elapsed().as_secs_f64()),
                    );

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

                    let commit_begin = Instant::now();
                    log_mem_stage(profile, "before_commit_execute", None);
                    let result = commit_builder.execute(transaction).await.map(|_| ());
                    log_mem_stage(
                        profile,
                        "after_commit_execute",
                        Some(commit_begin.elapsed().as_secs_f64()),
                    );
                    result
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

    // Inject io_uring write path when requested at runtime (--io-uring CLI flag).
    // Only available on Linux (io_uring is a Linux-specific syscall).
    #[cfg(all(feature = "io-uring", target_os = "linux"))]
    if config.use_io_uring {
        let mut store_params = lance_io::object_store::ObjectStoreParams::default();
        store_params.object_store_wrapper = Some(Arc::new(io_uring_store::IoUringWrapper));
        params.store_params = Some(store_params);
    }

    params
}

struct CompressionReader {
    inner: ArrowArrayStreamReader,
    schema: Arc<Schema>,
    profile: ProfileConfig,
    batch_count: usize,
}

impl CompressionReader {
    fn new(inner: ArrowArrayStreamReader, schema: Arc<Schema>, profile: ProfileConfig) -> Self {
        Self {
            inner,
            schema,
            profile,
            batch_count: 0,
        }
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
                self.batch_count += 1;
                if self.profile.enable_mem_profile
                    && (self.batch_count <= 3
                        || self.batch_count % self.profile.report_every_batches == 0)
                {
                    let rss = current_rss_kb().unwrap_or(0);
                    eprintln!(
                        "Lance FFI mem: stage=reader_next batch={} rows={} rss_kb={}",
                        self.batch_count,
                        batch.num_rows(),
                        rss
                    );
                }
                RecordBatch::try_new(self.schema.clone(), batch.columns().to_vec())
            })
        })
    }
}

enum ScatterGatherMsg {
    Chunk(Vec<RecordBatch>),
    End,
    Err(String),
}

struct ScatterGatherReader {
    schema: Arc<Schema>,
    rx: Receiver<ScatterGatherMsg>,
    current_chunk: Vec<RecordBatch>,
    chunk_idx: usize,
}

impl ScatterGatherReader {
    fn spawn(
        mut inner: ArrowArrayStreamReader,
        profile: ProfileConfig,
        batches_per_chunk: usize,
        queue_chunks: usize,
    ) -> Result<Self, String> {
        let schema = Arc::new(apply_compression_metadata(inner.schema().as_ref()));
        let (tx, rx) = sync_channel::<ScatterGatherMsg>(queue_chunks.max(1));
        let out_schema = schema.clone();
        let chunk_size = batches_per_chunk.max(1);

        thread::spawn(move || {
            let mut chunk = Vec::with_capacity(chunk_size);
            let mut seen_batches: usize = 0;
            loop {
                let next = inner.next();
                match next {
                    Some(Ok(batch)) => {
                        let out_batch = match RecordBatch::try_new(out_schema.clone(), batch.columns().to_vec()) {
                            Ok(b) => b,
                            Err(e) => {
                                let _ = tx.send(ScatterGatherMsg::Err(format!(
                                    "Scatter/gather schema rewrite failed: {}",
                                    e
                                )));
                                return;
                            }
                        };
                        seen_batches += 1;
                        if profile.enable_mem_profile
                            && (seen_batches <= 3
                                || seen_batches % profile.report_every_batches == 0)
                        {
                            let rss = current_rss_kb().unwrap_or(0);
                            eprintln!(
                                "Lance FFI mem: stage=sg_reader_next batch={} rows={} rss_kb={}",
                                seen_batches,
                                out_batch.num_rows(),
                                rss
                            );
                        }
                        chunk.push(out_batch);
                        if chunk.len() >= chunk_size {
                            if tx.send(ScatterGatherMsg::Chunk(std::mem::take(&mut chunk))).is_err() {
                                return;
                            }
                        }
                    }
                    Some(Err(e)) => {
                        let _ = tx.send(ScatterGatherMsg::Err(format!("Scatter/gather reader error: {}", e)));
                        return;
                    }
                    None => {
                        if !chunk.is_empty() {
                            let _ = tx.send(ScatterGatherMsg::Chunk(chunk));
                        }
                        let _ = tx.send(ScatterGatherMsg::End);
                        return;
                    }
                }
            }
        });

        Ok(Self {
            schema,
            rx,
            current_chunk: Vec::new(),
            chunk_idx: 0,
        })
    }
}

impl RecordBatchReader for ScatterGatherReader {
    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

impl Iterator for ScatterGatherReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.chunk_idx < self.current_chunk.len() {
            let out = self.current_chunk[self.chunk_idx].clone();
            self.chunk_idx += 1;
            return Some(Ok(out));
        }
        self.current_chunk.clear();
        self.chunk_idx = 0;

        match self.rx.recv() {
            Ok(ScatterGatherMsg::Chunk(chunk)) => {
                self.current_chunk = chunk;
                if self.current_chunk.is_empty() {
                    return self.next();
                }
                let out = self.current_chunk[0].clone();
                self.chunk_idx = 1;
                Some(Ok(out))
            }
            Ok(ScatterGatherMsg::End) => None,
            Ok(ScatterGatherMsg::Err(msg)) => Some(Err(ArrowError::ExternalError(Box::new(
                std::io::Error::new(std::io::ErrorKind::Other, msg),
            )))),
            Err(_) => None,
        }
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
                let flush_batch_threshold = writer.write_params.buffered_flush_batch_threshold.max(1);
                let flush_row_threshold = writer.write_params.buffered_flush_row_threshold.max(1);
                if batches.len() >= flush_batch_threshold || *pending_row_count >= flush_row_threshold {
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

/// Returns 1 if the background streaming task is still running, 0 if it has finished
/// (either successfully or with an error) or was never started. C++ uses this to detect
/// early task exit and unblock producers waiting on a full queue.
#[no_mangle]
pub extern "C" fn lance_writer_stream_is_alive(writer_ptr: *const LanceWriterHandle) -> c_int {
    if writer_ptr.is_null() { return 0; }
    let writer = unsafe { &*writer_ptr };
    match &writer.backend {
        WriterBackend::Streaming { task } => {
            task.as_ref().map(|t| if t.is_finished() { 0 } else { 1 }).unwrap_or(0)
        }
        _ => 0,
    }
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

/// Configure scatter/gather stream mode.
///
/// batches_per_chunk:
///   1 = disabled (default)
///   >1 = producer groups this many RecordBatches per queue chunk
///
/// queue_chunks:
///   Number of chunk slots in the bounded producer/consumer queue.
#[no_mangle]
pub extern "C" fn lance_writer_set_scatter_gather_config(
    writer_ptr: *mut LanceWriterHandle,
    batches_per_chunk: c_int,
    queue_chunks: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() { return 1; }
        let writer = unsafe { &mut *writer_ptr };
        if writer.closed { return 2; }
        if let WriterBackend::Streaming { task } = &writer.backend {
            if task.is_some() {
                eprintln!("Scatter/Gather Config Error: cannot change after stream start");
                return 5;
            }
        }
        if batches_per_chunk > 0 {
            writer.write_params.scatter_gather_batches = batches_per_chunk as usize;
        }
        if queue_chunks > 0 {
            writer.write_params.scatter_gather_queue_chunks = queue_chunks as usize;
        }
        0
    })).unwrap_or(3)
}

/// Configure flush thresholds for buffered backend.
#[no_mangle]
pub extern "C" fn lance_writer_set_buffered_flush_config(
    writer_ptr: *mut LanceWriterHandle,
    batch_threshold: c_int,
    row_threshold: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() { return 1; }
        let writer = unsafe { &mut *writer_ptr };
        if writer.closed { return 2; }
        if let WriterBackend::Streaming { task } = &writer.backend {
            if task.is_some() {
                eprintln!("Buffered Flush Config Error: cannot change after stream start");
                return 5;
            }
        }
        if batch_threshold > 0 {
            writer.write_params.buffered_flush_batch_threshold = batch_threshold as usize;
        }
        if row_threshold > 0 {
            writer.write_params.buffered_flush_row_threshold = row_threshold as usize;
        }
        0
    })).unwrap_or(3)
}

/// Configure Tokio runtime for streaming mode.
///
/// max_blocking_threads:
///   0 = keep current value
///   >0 = set blocking pool cap
///
/// Must be called before lance_writer_start_stream().
#[no_mangle]
pub extern "C" fn lance_writer_set_runtime_config(
    writer_ptr: *mut LanceWriterHandle,
    max_blocking_threads: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() { return 1; }
        let writer = unsafe { &mut *writer_ptr };
        if writer.closed { return 2; }

        let max_threads = if max_blocking_threads > 0 {
            max_blocking_threads as usize
        } else {
            0
        };
        match writer.set_runtime_config(max_threads) {
            Ok(_) => 0,
            Err(e) => {
                eprintln!("Runtime Config Error: {}", e);
                5
            }
        }
    })).unwrap_or(3)
}

#[no_mangle]
pub extern "C" fn lance_writer_set_profile_config(
    writer_ptr: *mut LanceWriterHandle,
    enable_mem_profile: c_int,
    report_every_batches: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() { return 1; }
        let writer = unsafe { &mut *writer_ptr };
        if writer.closed { return 2; }

        let every = if report_every_batches > 0 {
            report_every_batches as usize
        } else {
            0
        };
        match writer.set_profile_config(enable_mem_profile != 0, every) {
            Ok(_) => 0,
            Err(e) => {
                eprintln!("Profile Config Error: {}", e);
                5
            }
        }
    })).unwrap_or(3)
}

/// Enable or disable io_uring write path for this writer.
/// Must be called before the first batch is written.
/// Returns 0 on success, 1 if writer_ptr is null, 2 if already closed.
#[no_mangle]
pub extern "C" fn lance_writer_enable_io_uring(
    writer_ptr: *mut LanceWriterHandle,
    enabled: c_int,
) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() { return 1; }
        let writer = unsafe { &mut *writer_ptr };
        if writer.closed { return 2; }
        writer.write_params.use_io_uring = enabled != 0;
        if enabled != 0 {
            eprintln!("Lance FFI: io_uring write path {}", if cfg!(feature = "io-uring") {
                "enabled"
            } else {
                "requested but not compiled in (rebuild with --features io-uring)"
            });
        }
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
