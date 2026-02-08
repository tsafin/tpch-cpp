# Phase 1.5: Arrow C Data Interface Import Implementation

## Overview

This phase implements manual Arrow C Data Interface import in Rust to fix data loss in Lance writer. Currently, the FFI layer receives arrow array data but doesn't import it, resulting in empty Lance datasets.

## Problem Statement

- **Current state**: FFI structures received but not converted to Arrow arrays
- **Missing**: Implementation of C Data Interface to Arrow conversion
- **Result**: Empty Lance datasets (0 rows written) despite successful C++ streaming
- **Blocker**: Arrow 57 lacks public FFI import APIs

## Solution: Manual C Data Interface Implementation

Implement the Arrow C Data Interface specification directly in Rust to convert `FFI_ArrowArray` structures into usable Arrow arrays.

## Key C Data Interface Concepts

The C Data Interface (RFC 17) specifies a language-independent binary format for Arrow data:

```c
// From C Data Interface specification
typedef struct ArrowArray {
  int64_t length;           // Number of logical elements
  int64_t null_count;       // Count of null elements
  int64_t offset;           // Element offset
  int64_t n_buffers;        // Number of buffers
  int64_t n_children;       // Number of child arrays
  const void** buffers;     // Pointer to buffer pointers
  struct ArrowArray** children;  // Pointer to child array pointers
  struct ArrowArray* dictionary; // For dictionary-encoded arrays
  void (*release)(struct ArrowArray*);  // Release callback
  void* private_data;       // Implementation-specific data
};
```

### Buffer Interpretation by Type

**Primitive Types (int, float, etc.)**:
- Buffer 0: Null bitmap (bit-packed, one bit per element)
- Buffer 1: Data buffer (values stored sequentially)

**String/Binary Types**:
- Buffer 0: Null bitmap
- Buffer 1: Offset buffer (int32 values)
- Buffer 2: Data buffer (concatenated bytes)

**Dictionary Types**:
- Array has dictionary field
- Uses indices into dictionary

## Implementation Plan

### Phase 1.5.1: Type-Agnostic Infrastructure (1-2 hours)

Create utilities for reading FFI structures:

```rust
// Add to third_party/lance-ffi/src/lib.rs

/// Safe wrapper around FFI_ArrowArray C structure
struct SafeArrowArray {
    ffi: *mut FFI_ArrowArray,
}

impl SafeArrowArray {
    /// Read buffer pointer at index
    unsafe fn buffer_ptr(&self, index: usize) -> Option<*const u8> {
        if index >= (*self.ffi).n_buffers as usize {
            return None;
        }
        let buffers = (*self.ffi).buffers;
        if buffers.is_null() {
            return None;
        }
        Some(*buffers.add(index) as *const u8)
    }

    /// Read child array at index
    unsafe fn child(&self, index: usize) -> Option<*mut FFI_ArrowArray> {
        if index >= (*self.ffi).n_children as usize {
            return None;
        }
        let children = (*self.ffi).children;
        if children.is_null() {
            return None;
        }
        Some(*children.add(index))
    }

    /// Get field metadata from schema
    fn field_type(&self, schema_field: &ArrowField) -> Option<DataType> {
        // Extract type information from schema
        Some(schema_field.data_type().clone())
    }
}
```

### Phase 1.5.2: Primitive Type Arrays (2-3 hours)

Implement import for basic types used in TPCH:

```rust
/// Import a primitive type array (Int64, Float64, etc.)
fn import_primitive_array(
    safe_array: &SafeArrowArray,
    data_type: &DataType,
) -> Result<Arc<dyn Array>> {
    unsafe {
        let ffi_array = safe_array.ffi;
        let length = (*ffi_array).length as usize;

        // Buffer 0: Null bitmap
        let null_buffer = if let Some(ptr) = safe_array.buffer_ptr(0) {
            Some(Buffer::from_raw(ptr, length.div_ceil(8)))
        } else {
            None
        };

        // Buffer 1: Data values
        let data_ptr = safe_array.buffer_ptr(1)
            .ok_or("Missing data buffer")?;

        match data_type {
            DataType::Int64 => {
                let value_count = length;
                let data_buffer = Buffer::from_raw(data_ptr, value_count * 8);
                let array_data = ArrayData::new(
                    data_type.clone(),
                    length,
                    null_buffer,
                    Some(data_buffer),
                    0,
                    vec![],
                );
                Ok(Arc::new(Int64Array::from(array_data)))
            }
            DataType::Float64 => {
                // Similar to Int64
                // ...
            }
            DataType::Int32 => {
                // Similar pattern
                // ...
            }
            _ => Err(format!("Unsupported primitive type: {}", data_type))
        }
    }
}
```

### Phase 1.5.3: String/Binary Arrays (2-3 hours)

Implement variable-length types:

```rust
/// Import a string/binary type array
fn import_string_array(
    safe_array: &SafeArrowArray,
) -> Result<Arc<dyn Array>> {
    unsafe {
        let ffi_array = safe_array.ffi;
        let length = (*ffi_array).length as usize;

        // Buffer 0: Null bitmap
        let null_buffer = safe_array.buffer_ptr(0)
            .map(|ptr| Buffer::from_raw(ptr, length.div_ceil(8)));

        // Buffer 1: Offsets (int32)
        let offset_ptr = safe_array.buffer_ptr(1)
            .ok_or("Missing offset buffer")?;
        let offset_count = (length + 1) * 4;  // int32 per element + 1
        let offset_buffer = Buffer::from_raw(offset_ptr, offset_count);

        // Buffer 2: Data bytes
        let data_ptr = safe_array.buffer_ptr(2)
            .ok_or("Missing data buffer")?;
        // Get total byte length from last offset
        let last_offset = read_int32_at_offset(&offset_buffer, length);
        let data_buffer = Buffer::from_raw(data_ptr, last_offset as usize);

        let array_data = ArrayData::new(
            DataType::Utf8,
            length,
            null_buffer,
            None,
            0,
            vec![offset_buffer, data_buffer],
        );
        Ok(Arc::new(StringArray::from(array_data)))
    }
}
```

### Phase 1.5.4: Record Batch Assembly (1-2 hours)

Integrate all pieces into `import_ffi_batch()`:

```rust
/// Convert Arrow C Data Interface structures to RecordBatch
fn import_ffi_batch(
    arrow_array_ptr: *mut FFI_ArrowArray,
    arrow_schema_ptr: *mut FFI_ArrowSchema,
) -> Result<RecordBatch, String> {
    unsafe {
        // Import schema
        let ffi_schema = FFI_ArrowSchema::from_raw(arrow_schema_ptr);
        let schema = Schema::try_from(&ffi_schema)
            .map_err(|e| format!("Failed to convert FFI_ArrowSchema: {}", e))?;

        // Import array
        let ffi_array = FFI_ArrowArray::from_raw(arrow_array_ptr);
        let safe_array = SafeArrowArray {
            ffi: &mut ffi_array as *mut FFI_ArrowArray,
        };

        // Import each field as separate array
        let mut arrays = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            let child_array = safe_array.child(i)
                .ok_or(format!("Missing child array {}", i))?;
            let child_safe = SafeArrowArray { ffi: child_array };

            let array = match field.data_type() {
                DataType::Int64 => import_primitive_array(&child_safe, &DataType::Int64)?,
                DataType::Float64 => import_primitive_array(&child_safe, &DataType::Float64)?,
                DataType::Int32 => import_primitive_array(&child_safe, &DataType::Int32)?,
                DataType::Utf8 => import_string_array(&child_safe)?,
                dt => return Err(format!("Unsupported type: {}", dt)),
            };
            arrays.push(array);
        }

        // Release FFI structures
        if let Some(release) = ffi_schema.release {
            release(&mut ffi_schema as *mut FFI_ArrowSchema);
        }
        if let Some(release) = ffi_array.release {
            release(&mut ffi_array as *mut FFI_ArrowArray);
        }

        // Assemble RecordBatch
        RecordBatch::try_new(Arc::new(schema), arrays)
            .map_err(|e| format!("Failed to create RecordBatch: {}", e))
    }
}
```

### Phase 1.5.5: Testing & Validation (1 hour)

Test the implementation:

```bash
# Build with new implementation
RUSTC=/snap/bin/rustc /snap/bin/cargo build --release \
    --manifest-path=third_party/lance-ffi/Cargo.toml

# Build tpch benchmark
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTPCH_ENABLE_LANCE=ON ../..
cmake --build . -j$(nproc)

# Test Lance writer with each table
./tpch_benchmark --use-dbgen --format lance --table customer --scale-factor 1
./tpch_benchmark --use-dbgen --format lance --table orders --scale-factor 1
./tpch_benchmark --use-dbgen --format lance --table lineitem --scale-factor 1

# Verify datasets are not empty
find /tmp -name "*.lance" -type d | while read dir; do
    echo "Checking $dir:"
    ls -lah "$dir" | head -5
done
```

## Testing & Verification Checklist

- [ ] Rust code compiles without errors
- [ ] No warnings about unsafe code
- [ ] Lance writer receives batches (check logs)
- [ ] Error code 4 no longer occurs (FFI import successful)
- [ ] Lance datasets contain data (row count > 0)
- [ ] Customer table SF=1 produces 150K rows
- [ ] Orders table SF=1 produces 1.5M rows
- [ ] Lineitem table SF=1 produces 6M rows
- [ ] Datasets can be read by Lance reader
- [ ] Schema matches original Arrow schema

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Unsafe code bugs | Comprehensive testing, gradual type support |
| Memory layout errors | Reference C Data Interface spec closely |
| Buffer lifetime issues | Proper FFI release callbacks |
| Type mismatches | Test each type separately |

## Success Criteria

1. ✅ Lance datasets contain correct row counts (not 0)
2. ✅ No error code 4 in logs
3. ✅ Performance improvement verified (Phase 1 speedups should apply)
4. ✅ All TPCH tables (customer, orders, lineitem) work correctly
5. ✅ Scaling works from SF=1 to SF=5+

## Timeline Estimate

- Phase 1.5.1: 1-2 hours
- Phase 1.5.2: 2-3 hours
- Phase 1.5.3: 2-3 hours
- Phase 1.5.4: 1-2 hours
- Phase 1.5.5: 1 hour
- **Total: 7-11 hours** (can parallelize some sections)

## Expected Outcomes

After completing Phase 1.5:
1. FFI import fully functional ✅
2. Lance datasets contain actual data ✅
3. Ready for Phase 2 optimization ✅
4. Streaming architecture validated end-to-end ✅

---

## Phase 2 (Unblocked by this phase)

After Phase 1.5 completes:
- Remove Arrow Parquet writer bypass
- Use native Lance format writing
- Expected 2-5x additional speedup
