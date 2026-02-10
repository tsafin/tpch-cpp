//! Lance FFI Bridge (Phase 2 - Native Lance Writing via Arrow FFI)
//! FFI Bridge for Arrow -> Lance

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::pin::Pin;

use arrow::ffi::{FFI_ArrowSchema, FFI_ArrowArray};
use arrow::record_batch::RecordBatch;
use arrow::array::RecordBatchIterator;
use arrow::datatypes::{Schema, DataType, Field};
use arrow::error::ArrowError;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use futures::StreamExt;

// Lance Dependencies
use lance::dataset::{WriteParams, WriteMode};
use lance::deps::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use lance::deps::datafusion::physical_plan::RecordBatchStream;
use lance::deps::datafusion::error::DataFusionError;

fn apply_compression_metadata(schema: &Schema) -> Schema {
    let fields: Vec<Field> = schema.fields().iter().map(|field| {
        let mut metadata = field.metadata().clone();
        
        // Use lz4 for fast compression (quick and effective)
        metadata.insert("lance-encoding:compression".to_string(), "lz4".to_string());
        
        // Enable Byte Stream Split for better float compression
        match field.data_type() {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                 metadata.insert("lance-encoding:bss".to_string(), "auto".to_string());
            },
            _ => {}
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
        // Use Option<Sender> to allow closing the channel
        sender: Option<mpsc::Sender<Result<RecordBatch, ArrowError>>>,
        // Receiver held here until the first batch arrives (lazy init)
        receiver: Option<mpsc::Receiver<Result<RecordBatch, ArrowError>>>,
        // Background task handle
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
}

const FLUSH_BATCH_THRESHOLD: usize = 200;
const FLUSH_ROW_THRESHOLD: usize = 1_000_000;

impl LanceWriterHandle {
    fn new(uri: String, use_streaming: bool) -> Result<Self, String> {
        let runtime = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;

        let backend = if use_streaming {
            let (sender, receiver) = mpsc::channel(500);
            WriterBackend::Streaming {
                sender: Some(sender),
                receiver: Some(receiver),
                task: None, // Lazy init
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
        match &mut self.backend {
            WriterBackend::Buffered { batches, dataset_initialized, fragment_count, pending_row_count } => {
                if batches.is_empty() { return Ok(()); }
                let uri = self.uri.clone();
                let drain_batches = std::mem::take(batches);
                let flush_batch_count = drain_batches.len();
                let flush_row_count = *pending_row_count;
                let schema_ref = drain_batches[0].schema();
                
                let mode = if *dataset_initialized { WriteMode::Append } else { WriteMode::Overwrite };
                let write_params = WriteParams {
                    max_rows_per_group: 8192, max_rows_per_file: 50_000_000, mode, ..Default::default()
                };

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

    fn send_batch(&mut self, batch: RecordBatch) -> Result<(), String> {
        match &mut self.backend {
            WriterBackend::Streaming { sender, receiver, task } => {
                // Lazy init
                if task.is_none() {
                     let rx = receiver.take().ok_or("Receiver is missing in streaming backend")?;
                     let schema = batch.schema();
                     let uri_clone = self.uri.clone();
                     
                     eprintln!("Lance FFI: Starting streaming background task with schema...");

                     let task_handle = self.runtime.spawn(async move {
                         // Convert Receiver into Stream<Item=Result<RecordBatch, DataFusionError>>
                         let receiver_stream = ReceiverStream::new(rx);
                         let mapped_stream = receiver_stream.map(|res| res.map_err(DataFusionError::from));
                         
                         // Create Adapter
                         let stream_adapter = RecordBatchStreamAdapter::new(schema, mapped_stream);
                         let source: Pin<Box<dyn RecordBatchStream + Send>> = Box::pin(stream_adapter);
                         
                         let write_params = WriteParams {
                            max_rows_per_group: 8192,
                            max_rows_per_file: 50_000_000,
                            mode: WriteMode::Overwrite,
                            ..Default::default()
                        };

                        lance::dataset::InsertBuilder::new(&uri_clone)
                            .with_params(&write_params)
                            .execute_stream(source)
                            .await
                            .map(|_| ())
                     });

                     *task = Some(task_handle);
                }
                
                if let Some(tx) = sender {
                    let result = self.runtime.block_on(async {
                        tx.send(Ok(batch)).await
                    });
                    if result.is_err() {
                        Err("Failed to send batch: receiver dropped".to_string())
                    } else {
                        Ok(())
                    }
                } else {
                    Err("Sender is closed".to_string())
                }
            },
            _ => Ok(())
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
pub extern "C" fn lance_writer_close(writer_ptr: *mut LanceWriterHandle) -> c_int {
    catch_unwind(AssertUnwindSafe(|| {
        if writer_ptr.is_null() { return 1; }
        let writer = unsafe { &mut *writer_ptr };
        if writer.closed { return 2; }

        let res = match &mut writer.backend {
            WriterBackend::Buffered { .. } => writer.flush_batches(),
            WriterBackend::Streaming { sender, task, .. } => {
                // Close channel
                *sender = None;
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