# FFI Ownership Mismatch Analysis - PR #5

## Critical Issue: Double-Free Between C++ and Rust

### The Problem

The current implementation has a **double-free vulnerability** due to conflicting ownership of FFI structures between C++ and Rust.

### Ownership Flow (Current - BROKEN)

```
C++ Side (lance_writer.cpp)
├─ ExportRecordBatch() creates FFI_ArrowArray and FFI_ArrowSchema on heap
├─ Passes pointers to Rust via lance_writer_write_batch()
└─ After Rust returns:
   ├─ Calls arr->release(arr) [Line 93]
   ├─ Calls sch->release(sch) [Line 100]
   └─ delete arr; delete sch; [Lines 95, 102]

Rust Side (lance-ffi/src/lib.rs)
├─ Receives FFI pointers in lance_writer_write_batch()
├─ Calls FFI_ArrowSchema::from_raw(arrow_schema_ptr) [Line 316]
│  └─ Takes OWNERSHIP of the pointer
│  └─ Will call release() when dropped
└─ Uses in import_ffi_batch()
   └─ When ffi_schema goes out of scope:
      └─ Calls release() callback [FIRST RELEASE]
```

**Result**: Release called TWICE, delete called TWICE = **DOUBLE-FREE / UAF**

### Root Cause

Arrow's C Data Interface `from_raw()` documentation states:
> "This function takes ownership of the structures pointed to by the arguments."

By calling `from_raw()`, Rust takes ownership and becomes responsible for calling the release callbacks.

C++ should NOT call release() or delete after passing ownership to Rust.

### Correct Ownership Models

#### Option A: Rust Owns (Recommended)

C++ should:
1. Allocate FFI structures
2. Pass to Rust
3. **NOT** call release() or delete
4. Let Rust handle cleanup via Drop

```rust
// Rust side - OWNS the structures
let ffi_schema = FFI_ArrowSchema::from_raw(arrow_schema_ptr);
// When ffi_schema is dropped, release() is called automatically
```

```cpp
// C++ side - TRANSFER ownership
auto [array_ptr, schema_ptr] = batch_to_ffi(batch);
int result = lance_writer_write_batch(raw_writer, array_ptr, schema_ptr);
// Do NOT call free_ffi_structures() - Rust now owns these
// No delete[] or release() calls
```

#### Option B: C++ Owns (Borrowed References)

Rust should:
1. Use borrowed references (not from_raw)
2. NOT take ownership
3. Return without calling release()

C++ remains responsible for cleanup.

```rust
// Rust side - BORROWS the structures
let ffi_schema = FFI_ArrowSchema::from_raw(arrow_schema_ptr);
mem::forget(ffi_schema); // Forget to prevent Drop from calling release()
```

### Recommended Fix: Option A (Rust Owns)

**Rationale**:
- Simpler semantics - Rust's RAII handles lifetime
- Matches Arrow C Data Interface design intent
- Eliminates C++ cleanup logic

**Changes Required**:

1. **C++ (lance_writer.cpp)**:
   - Remove `free_ffi_structures()` calls after `lance_writer_write_batch()`
   - DO NOT call release() or delete
   - Remove the `free_ffi_structures()` method entirely

2. **Rust (lance-ffi/src/lib.rs)**:
   - Keep current `FFI_ArrowSchema::from_raw()` (takes ownership)
   - Ensure `FFI_ArrowArray` handling is also via `from_raw()` if needed
   - Let Drop traits handle release() callbacks

### Implementation Plan

**Phase 1: Document Current State**
- [ ] Add safety comments explaining ownership model
- [ ] Document which side owns what

**Phase 2: Fix C++ Side**
- [ ] Remove `free_ffi_structures()` calls from `write_batch()`
- [ ] Remove `free_ffi_structures()` calls from error path
- [ ] Remove `free_ffi_structures()` method
- [ ] Update comments to explain Rust now owns FFI structures

**Phase 3: Verify Rust Side**
- [ ] Confirm `from_raw()` is used correctly
- [ ] Verify drop behavior calls release callbacks
- [ ] Add safety comments

**Phase 4: Testing**
- [ ] Run memory leak detection tools (valgrind, ASAN)
- [ ] Test with various batch sizes
- [ ] Monitor for UAF/double-free errors

### Files to Modify

1. `src/writers/lance_writer.cpp`
   - Remove `free_ffi_structures()` method
   - Remove calls after `lance_writer_write_batch()`
   - Update comments

2. `third_party/lance-ffi/src/lib.rs`
   - Add safety comments about ownership
   - Ensure Drop is implemented correctly

### Risk Assessment

**Current Risk**: HIGH
- Double-free can cause crashes or data corruption
- May not manifest until specific conditions trigger it
- Difficult to debug without memory tools

**Fix Risk**: LOW
- Only removes cleanup code from C++
- Rust's ownership model is well-established
- Follows Arrow C Data Interface design

### References

- [Arrow C Data Interface Specification](https://arrow.apache.org/docs/format/CDataInterface.html)
- Arrow Rust FFI documentation: `FFI_ArrowSchema::from_raw()`
- Rust Drop trait semantics

---

## Summary

**Status**: CRITICAL - Double-free vulnerability identified

**Solution**: Transfer FFI ownership to Rust, remove C++ cleanup

**Action**: Remove `free_ffi_structures()` calls and method from C++
