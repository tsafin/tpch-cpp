# Phase 1.5 Quick Start: Arrow FFI Import Fix

## What to Do

Fix the broken Arrow C Data Interface import in Rust FFI layer to enable actual data writing to Lance datasets.

## Current Problem

- Rust FFI receives Arrow batches from C++ ✅
- But doesn't import the actual array data ❌
- Result: Empty Lance datasets (0 rows) ❌

## Solution Overview

Implement manual C Data Interface import in Rust using the specification to convert `FFI_ArrowArray` structures into Arrow arrays.

## Getting Started

### 1. Understand Current Code (30 min)

Read these files to understand the architecture:

```bash
# Understanding the broken part
cat third_party/lance-ffi/src/lib.rs | grep -A 25 "fn import_ffi_batch"

# Understanding what needs to work
cat src/writers/lance_writer.cpp | grep -A 20 "lance_writer_write_batch"

# Seeing the C++ side (which works)
cat src/writers/lance_writer.cpp | grep -B 5 -A 10 "ExportRecordBatch"
```

Key insight: C++ correctly exports the data → Rust needs to import it.

### 2. Study Reference Material (1 hour)

- [ ] Read Arrow C Data Interface RFC: https://arrow.apache.org/rfc/
- [ ] Understand buffer layouts for: Int64, Float64, String
- [ ] Review LANCE_FFI_PHASE1_5_IMPLEMENTATION.md sections 1-3

### 3. Start Implementation

#### Step 1: Create Safe FFI Wrapper (1-2 hours)

File: `third_party/lance-ffi/src/lib.rs`

Add around line 51 (before `import_ffi_batch`):

```rust
/// Safe wrapper around FFI_ArrowArray C structure
/// Provides methods to safely read buffer pointers and child arrays
struct SafeArrowArray {
    ffi: *mut FFI_ArrowArray,
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
    unsafe fn child(&self, index: usize) -> Option<*mut FFI_ArrowArray> {
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

        Some(*ffi_array.children.add(index))
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
```

**Test this compiles**:
```bash
cd third_party/lance-ffi
RUSTC=/snap/bin/rustc /snap/bin/cargo check
```

#### Step 2: Implement Primitive Type Import (2-3 hours)

Add after SafeArrowArray impl:

```rust
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
        let null_buffer = safe_array
            .buffer_ptr(0)
            .map(|ptr| {
                let byte_count = (length + 7) / 8; // Ceiling division
                Buffer::from_raw_parts(
                    NonNull::new(ptr as *mut u8).unwrap(),
                    byte_count,
                    Arc::new(()),
                )
            });

        // Buffer 1: Data values
        let data_ptr = safe_array
            .buffer_ptr(1)
            .ok_or("Missing data buffer for primitive array")?;

        match field.data_type() {
            DataType::Int64 => {
                let byte_count = length * std::mem::size_of::<i64>();
                let data_buffer = Buffer::from_raw_parts(
                    NonNull::new(data_ptr as *mut u8).unwrap(),
                    byte_count,
                    Arc::new(()),
                );

                let array_data = ArrayData::builder(DataType::Int64)
                    .len(length)
                    .buffers(vec![null_buffer.clone().unwrap_or_default(), data_buffer])
                    .null_count(safe_array.null_count() as usize)
                    .build_unchecked();

                Ok(Arc::new(Int64Array::from(array_data)))
            }
            DataType::Float64 => {
                let byte_count = length * std::mem::size_of::<f64>();
                let data_buffer = Buffer::from_raw_parts(
                    NonNull::new(data_ptr as *mut u8).unwrap(),
                    byte_count,
                    Arc::new(()),
                );

                let array_data = ArrayData::builder(DataType::Float64)
                    .len(length)
                    .buffers(vec![null_buffer.clone().unwrap_or_default(), data_buffer])
                    .null_count(safe_array.null_count() as usize)
                    .build_unchecked();

                Ok(Arc::new(Float64Array::from(array_data)))
            }
            DataType::Int32 => {
                let byte_count = length * std::mem::size_of::<i32>();
                let data_buffer = Buffer::from_raw_parts(
                    NonNull::new(data_ptr as *mut u8).unwrap(),
                    byte_count,
                    Arc::new(()),
                );

                let array_data = ArrayData::builder(DataType::Int32)
                    .len(length)
                    .buffers(vec![null_buffer.clone().unwrap_or_default(), data_buffer])
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
```

**Test this compiles and is called**:
```bash
RUSTC=/snap/bin/rustc /snap/bin/cargo check
```

#### Step 3: Implement String Array Import (2-3 hours)

Add after primitive type function:

```rust
/// Import a string/binary type array
fn import_string_array(
    safe_array: &SafeArrowArray,
    field: &Field,
) -> Result<Arc<dyn Array>, String> {
    unsafe {
        let length = safe_array.length() as usize;
        if length == 0 {
            return Err("Cannot import array with 0 length".to_string());
        }

        // Buffer 0: Null bitmap
        let null_buffer = safe_array
            .buffer_ptr(0)
            .map(|ptr| {
                let byte_count = (length + 7) / 8;
                Buffer::from_raw_parts(
                    NonNull::new(ptr as *mut u8).unwrap(),
                    byte_count,
                    Arc::new(()),
                )
            });

        // Buffer 1: Offsets (int32 per element + 1)
        let offset_ptr = safe_array
            .buffer_ptr(1)
            .ok_or("Missing offset buffer for string array")?;
        let offset_byte_count = (length + 1) * std::mem::size_of::<i32>();
        let offset_buffer = Buffer::from_raw_parts(
            NonNull::new(offset_ptr as *mut u8).unwrap(),
            offset_byte_count,
            Arc::new(()),
        );

        // Buffer 2: Data bytes
        let data_ptr = safe_array
            .buffer_ptr(2)
            .ok_or("Missing data buffer for string array")?;

        // Get total byte length from last offset
        let offsets = offset_buffer.typed_data::<i32>();
        let data_byte_count = if !offsets.is_empty() {
            offsets[length] as usize
        } else {
            0
        };

        let data_buffer = Buffer::from_raw_parts(
            NonNull::new(data_ptr as *mut u8).unwrap(),
            data_byte_count,
            Arc::new(()),
        );

        let array_data = ArrayData::builder(DataType::Utf8)
            .len(length)
            .buffers(vec![
                null_buffer.clone().unwrap_or_default(),
                offset_buffer,
                data_buffer,
            ])
            .null_count(safe_array.null_count() as usize)
            .build_unchecked();

        Ok(Arc::new(StringArray::from(array_data)))
    }
}
```

**Test this compiles**:
```bash
RUSTC=/snap/bin/rustc /snap/bin/cargo check
```

#### Step 4: Fix the Main import_ffi_batch Function (1-2 hours)

Replace the entire `import_ffi_batch` function (lines 60-83) with:

```rust
fn import_ffi_batch(
    arrow_array_ptr: *mut FFI_ArrowArray,
    arrow_schema_ptr: *mut FFI_ArrowSchema,
) -> Result<RecordBatch, String> {
    unsafe {
        // Import FFI schema
        let ffi_schema = FFI_ArrowSchema::from_raw(arrow_schema_ptr);
        let schema = Schema::try_from(&ffi_schema)
            .map_err(|e| format!("Failed to convert FFI_ArrowSchema to Schema: {}", e))?;

        // Get root array
        let ffi_array = FFI_ArrowArray::from_raw(arrow_array_ptr);
        let safe_array = SafeArrowArray {
            ffi: arrow_array_ptr,
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

        // Release FFI structures if they have release callbacks
        if let Some(release_cb) = ffi_schema.release {
            release_cb(arrow_schema_ptr);
        }
        if let Some(release_cb) = ffi_array.release {
            release_cb(arrow_array_ptr);
        }

        // Create and return RecordBatch
        RecordBatch::try_new(Arc::new(schema), arrays)
            .map_err(|e| format!("Failed to create RecordBatch: {}", e))
    }
}
```

**Test this compiles**:
```bash
RUSTC=/snap/bin/rustc /snap/bin/cargo check
```

#### Step 5: Build and Test

```bash
# Build Rust FFI library
cd third_party/lance-ffi
RUSTC=/snap/bin/rustc /snap/bin/cargo build --release

# Build full project
cd /home/tsafin/src/tpch-cpp
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTPCH_ENABLE_LANCE=ON ../..
cmake --build . -j$(nproc)

# Test with customer table
./tpch_benchmark --use-dbgen --format lance --table customer --scale-factor 1

# Check Lance dataset has data
ls -lh /tmp/tpch_sf1_customer.lance/
# Should contain data files, not be empty
```

## Checklist

- [ ] Understand the problem (read LANCE_FFI_STATUS_SUMMARY.md)
- [ ] Review Arrow C Data Interface spec section on buffer layouts
- [ ] Add SafeArrowArray wrapper struct
- [ ] Implement import_primitive_array function
- [ ] Implement import_string_array function
- [ ] Replace import_ffi_batch with working implementation
- [ ] Verify it compiles with `cargo check`
- [ ] Build full project
- [ ] Test customer table (SF=1): should produce 150k rows
- [ ] Test orders table (SF=1): should produce 1.5M rows
- [ ] Test lineitem table (SF=1): should produce 6M rows
- [ ] Verify error code 4 no longer appears
- [ ] Commit changes with message: "fix: Implement Arrow FFI import for Lance writer"

## If You Get Stuck

### Compilation Errors

**Error: "cannot find function xxx"**
- Make sure you're using `/snap/bin/rustc` for correct version

**Error: "unsafe block required"**
- Ensure code is inside `unsafe { }` block

### Runtime Errors

**Error code 4 still appears**
- FFI import still failing, check error messages in stderr
- Add eprintln! for debugging

**Lance dataset still empty**
- Check row_count in FFI logs matches C++ logs
- Verify buffer pointers are non-null

## Expected Results

### Before Fix
```
Lance FFI: Accumulated batch 1 (1000 rows total)
Lance FFI: Successfully wrote Lance dataset: /tmp/dataset.lance (1 batches, 1000 rows)
# But dataset actually contains 0 rows
```

### After Fix
```
Lance FFI: Accumulated batch 1 (1000 rows total)
Lance FFI: Successfully wrote Lance dataset: /tmp/dataset.lance (1 batches, 1000 rows)
# And dataset actually contains 1000 rows
```

## Next After Phase 1.5

Once FFI import is working:
- [ ] Run full scaling tests (SF=1,5,10)
- [ ] Benchmark vs Parquet baseline
- [ ] Document performance results
- [ ] Plan Phase 2: Remove Parquet bypass, use native Lance format

---

**Time estimate**: 7-11 hours total
**Difficulty**: Medium (dealing with FFI and unsafe code)
**Impact**: Critical (unblocks Phase 2 optimization)
