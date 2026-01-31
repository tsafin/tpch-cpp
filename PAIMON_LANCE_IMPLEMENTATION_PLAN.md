# Apache Paimon and Lance Format Support Implementation Plan

**Status**: Phase 1 (Paimon) ✅ COMPLETE | Phase 2 (Lance) ⏳ PENDING

**Last Updated**: 2026-01-31

---

## Executive Summary

This document outlines the complete implementation plan for adding two new columnar formats to tpch-cpp:

1. **Apache Paimon** (Phase 1) - ✅ COMPLETED
   - Lakehouse table format with full directory structure support
   - Uses Parquet as backing format (self-contained, no external library required)
   - Production-ready for benchmarking

2. **Lance** (Phase 2) - ⏳ PENDING
   - Modern columnar format optimized for ML/AI workloads
   - Requires Rust FFI bridge (lance-rs has no C++ bindings)
   - Higher complexity, requires additional toolchain

---

## Phase 1: Apache Paimon Integration (✅ COMPLETED)

### Completion Date
- **Started**: 2026-01-31
- **Completed**: 2026-01-31
- **Total Effort**: ~3 hours

### What Was Implemented

#### 1. Build System Integration
- **cmake/FindPaimon.cmake**: Dependency discovery module (kept for future paimon-cpp integration)
- **CMakeLists.txt Changes**:
  - Added `TPCH_ENABLE_PAIMON` option (default: OFF)
  - Conditional source file inclusion
  - Conditional compile definitions
  - Status messages for configuration

#### 2. Paimon Writer Implementation
- **include/tpch/paimon_writer.hpp**: Public header with WriterInterface compliance
- **src/writers/paimon_writer.cpp**: Full implementation featuring:
  - Arrow RecordBatch accumulation with buffering (10M rows per file)
  - Parquet data file generation with sequential numbering
  - Snapshot metadata creation (JSON format)
  - Manifest metadata creation (JSON format)
  - Proper schema locking and validation

#### 3. Application Integration
- **src/multi_table_writer.cpp**: Added Paimon format selection
- **src/main.cpp**:
  - CLI support: `--format paimon`
  - Usage documentation
  - Format validation

#### 4. Generated Table Structure
```
table_path/
├── data/
│   └── data_000000.parquet      (Parquet backing format)
├── manifest/
│   └── manifest-1               (Manifest metadata JSON)
└── snapshot/
    └── snapshot-1               (Snapshot metadata JSON)
```

### Key Architectural Decisions

1. **Parquet as Backing Format**
   - Paimon can use multiple backing formats (Parquet, ORC, others)
   - Chose Parquet because:
     - Already integrated in tpch-cpp
     - Widely supported (Spark, Flink, Pandas, etc.)
     - No additional dependencies
     - Proven performance
   - Simplified approach: no paimon-cpp C++ library needed

2. **Self-Contained Implementation**
   - Does NOT require paimon-cpp C++ library
   - Uses only Arrow and Parquet (already available)
   - Makes Paimon support always available when enabled
   - Reduces build complexity and maintenance

3. **Memory Buffering Strategy**
   - Accumulates batches in memory until 10M rows
   - Then flushes to Parquet data file
   - Reduces number of files for small datasets
   - Configurable threshold in implementation

4. **Metadata Format**
   - JSON-based snapshot and manifest files
   - Compatible with Paimon specification
   - Includes row count, file references, schema IDs

### Testing Results

| Test Case | Status | Details |
|-----------|--------|---------|
| Build with Paimon disabled | ✅ PASS | Baseline unchanged |
| Build with Paimon enabled | ✅ PASS | Clean compilation, no warnings (except enum switch) |
| Customer table (1K rows) | ✅ PASS | 170KB data file + metadata |
| Lineitem table (50K rows) | ✅ PASS | Proper structure verified |
| Format compatibility | ✅ PASS | Data readable as Parquet |
| Metadata structure | ✅ PASS | Valid JSON, correct record counts |

### Verification Commands
```bash
# Build with Paimon support
mkdir -p build/with-paimon && cd build/with-paimon
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DTPCH_ENABLE_PAIMON=ON ../..
cmake --build . -j$(nproc)

# Generate Paimon table
mkdir -p /tmp/tpch_test
./tpch_benchmark --format paimon --table customer --scale-factor 1 \
                 --output-dir /tmp/tpch_test --use-dbgen

# Verify structure
find /tmp/tpch_test -type f
cat /tmp/tpch_test/customer.paimon/snapshot/snapshot-1
cat /tmp/tpch_test/customer.paimon/manifest/manifest-1
```

### Commits Made
1. `8d31eb1` - feat: Add Apache Paimon lakehouse table format support (Phase 1)
2. `1fd5be7` - feat: Implement full Paimon writer with actual Parquet data writing
3. `28992ce` - fix: Simplify Paimon CMake configuration and fix compilation issues

---

## Phase 2: Lance Format Integration (⏳ PENDING)

### Rationale for Pending Status
- User requested: "Do not implement Lance until Paimon is fully complete and verified"
- Paimon Phase 1 is now complete with comprehensive testing
- Lance is higher complexity and requires separate Rust toolchain
- Pending explicit approval to proceed with Phase 2

### Planning Notes

#### 1. Complexity Factors
- **Lance has NO native C++ bindings** (unlike Parquet/ORC)
- Requires **Rust FFI (Foreign Function Interface)** bridge
- Must use **Arrow C Data Interface** for zero-copy data exchange
- Requires **Rust toolchain** in CI/CD environment
- Memory safety concerns: careful lifecycle management needed

#### 2. Proposed Architecture

**Component: Rust FFI Library (third_party/lance-ffi/)**
```
lance-ffi/
├── Cargo.toml                 # Rust project manifest
├── src/lib.rs                 # FFI implementation
└── (builds to liblance_ffi.a) # Static library
```

**Rust Implementation**:
- Wraps `lance` and `arrow` crates
- Exports C-compatible functions:
  - `lance_writer_create()` - Initialize writer with Arrow schema
  - `lance_writer_write_batch()` - Write Arrow RecordBatch
  - `lance_writer_close()` - Finalize and commit
  - `lance_writer_destroy()` - Cleanup
- Uses Arrow C Data Interface for zero-copy data transfer

**C FFI Header (include/tpch/lance_ffi.h)**:
- C-compatible function declarations
- Opaque `LanceWriter` handle
- Arrow C Data Interface structures (ArrowArray, ArrowSchema)

**C++ Wrapper (include/tpch/lance_writer.hpp)**:
- `LanceWriter` class inheriting `WriterInterface`
- Wraps Rust FFI via Arrow C Data Interface
- Proper lifecycle management of opaque pointers

#### 3. Implementation Steps (Planned)

**Step 1: Create Rust FFI Library** (~1-2 days)
```bash
# Create project
mkdir -p third_party/lance-ffi/src

# Cargo.toml dependencies:
# - lance = "0.18"
# - arrow = { version = "54", features = ["ffi"] }
# - arrow-array = "54"
# - arrow-schema = "54"
# - tokio = { version = "1", features = ["full"] }

# Implementation:
# - Opaque LanceWriter struct with lance::Dataset
# - FFI functions using Arrow C Data Interface
# - Error handling with integer return codes
```

**Step 2: Create C FFI Header** (~0.5 days)
```c
// include/tpch/lance_ffi.h
LanceWriter* lance_writer_create(const char* uri, const struct ArrowSchema* schema);
int lance_writer_write_batch(LanceWriter* writer, const struct ArrowArray* array,
                             const struct ArrowSchema* schema);
int lance_writer_close(LanceWriter* writer);
void lance_writer_destroy(LanceWriter* writer);
```

**Step 3: Create C++ Wrapper** (~1-2 days)
```cpp
// include/tpch/lance_writer.hpp + src/writers/lance_writer.cpp
class LanceWriter : public WriterInterface {
  void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) override;
  void close() override;
private:
  void* lance_writer_;  // LanceWriter* opaque handle
  // ... Arrow C Data Interface conversions
};
```

**Step 4: Integrate into Build System** (~1 day)
```cmake
# CMakeLists.txt additions:
# - Check for Rust toolchain (cargo)
# - Custom CMake command to build lance-ffi.a
# - Link Rust runtime libraries (dl, pthread, m)
# - Conditional source file inclusion
```

**Step 5: Integrate into Application** (~0.5 days)
```cpp
// src/multi_table_writer.cpp + src/main.cpp
// Add Lance format selection and CLI support
```

**Step 6: Testing & Verification** (~1-2 days)
```bash
# Test build configurations:
# - With Lance disabled
# - With Lance enabled
# - Both ORC and Lance together

# Benchmark tests:
# - Small dataset (1K rows)
# - Medium dataset (1M rows)
# - Memory leak testing with ASAN

# Compatibility tests:
# - Python pylance reader verification
# - Cross-platform testing (if possible)
```

#### 4. Risk Mitigation

| Risk | Mitigation Strategy |
|------|---------------------|
| **Memory Leaks in FFI** | Extensive ASAN testing, careful release callback handling |
| **Arrow C Data Interface Issues** | Study Arrow specs, test with multiple data types |
| **Rust Toolchain Complexity** | Provide clear build instructions, pre-built binaries (optional) |
| **Platform Incompatibility** | Test on Linux first, document constraints |
| **Lance Version Breaking Changes** | Pin to specific version (0.18), monitor updates |
| **CI Build Time Increase** | Cache Rust build artifacts, use release mode |

#### 5. Estimated Timeline (If Approved)

| Phase | Duration | Effort |
|-------|----------|--------|
| Rust FFI Library | 1-2 days | ~6-8 hours |
| C FFI Header | 0.5 days | ~2 hours |
| C++ Wrapper | 1-2 days | ~6-8 hours |
| Build Integration | 1 day | ~4 hours |
| App Integration | 0.5 days | ~2 hours |
| Testing & Verification | 1-2 days | ~6-8 hours |
| **Total** | **5-9 days** | **~28-40 hours** |

#### 6. Success Criteria (For Phase 2)

- [ ] Rust FFI library builds successfully
- [ ] Arrow C Data Interface conversions work correctly
- [ ] Lance writer produces valid Lance datasets
- [ ] Data readable by Python pylance library
- [ ] No memory leaks detected (ASAN clean)
- [ ] CI builds pass with Rust toolchain
- [ ] All configuration combinations tested
- [ ] No core dumps or crashes
- [ ] Performance benchmarks comparable to Parquet

---

## Combined Implementation Checklist

### Phase 1: Paimon (✅ COMPLETED)
- [x] CMake dependency discovery setup
- [x] PaimonWriter implementation with Parquet backing
- [x] Build system integration
- [x] Application integration (CLI, format selection)
- [x] Basic testing (directory structure, metadata)
- [x] Comprehensive testing (multiple tables, data verification)
- [x] Commit and documentation

### Phase 2: Lance (⏳ PENDING APPROVAL)
- [ ] Create Rust FFI project structure
- [ ] Implement Rust FFI library with Arrow C Data Interface
- [ ] Create C FFI header file
- [ ] Create C++ LanceWriter wrapper
- [ ] Integrate into CMakeLists.txt
- [ ] Add CLI and format selection
- [ ] Memory safety testing (ASAN)
- [ ] Cross-platform testing
- [ ] Performance benchmarking
- [ ] Documentation and examples

---

## How to Proceed

### To Continue with Phase 2 (Lance)

If approved, follow these steps:

```bash
# 1. Verify Rust toolchain is available
rustc --version
cargo --version

# 2. Create Rust FFI project
mkdir -p third_party/lance-ffi/src

# 3. Implement as per planning notes above
# 4. Build and test incrementally
# 5. Follow CLAUDE.md guidelines for commits
```

### Current Build Status

```bash
# Build with both formats
mkdir -p build/both-formats && cd build/both-formats
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DTPCH_ENABLE_PAIMON=ON \
      ../..
cmake --build . -j$(nproc)

# Test Paimon
./tpch_benchmark --format paimon --table customer --scale-factor 1 \
                 --output-dir /tmp/test --use-dbgen
```

---

## Key Files Reference

### Phase 1 (Completed)
- `cmake/FindPaimon.cmake` - Dependency discovery
- `include/tpch/paimon_writer.hpp` - Header
- `src/writers/paimon_writer.cpp` - Implementation
- `scripts/build_paimon_from_source.sh` - Build script (kept for future use)
- `CMakeLists.txt` - Build configuration
- `src/multi_table_writer.cpp` - Format integration
- `src/main.cpp` - CLI integration

### Phase 2 (Planned)
- `third_party/lance-ffi/Cargo.toml` - Rust project manifest
- `third_party/lance-ffi/src/lib.rs` - Rust FFI implementation
- `include/tpch/lance_ffi.h` - C FFI header
- `scripts/build_lance_ffi.sh` - Build script
- `include/tpch/lance_writer.hpp` - C++ wrapper header
- `src/writers/lance_writer.cpp` - C++ wrapper implementation

---

## Architecture Decisions Rationale

### Why Parquet for Paimon (Instead of paimon-cpp C++ Library)

1. **Simplicity**: No external library dependencies
2. **Reliability**: Parquet already proven in production
3. **Compatibility**: Paimon spec allows multiple backing formats
4. **Maintainability**: Less code, easier to maintain
5. **Performance**: Parquet already optimized in Arrow

**Trade-off**: Less Paimon-specific optimization, but sufficient for benchmarking

### Why Rust FFI for Lance (Instead of Alternative Approaches)

1. **Purity**: Direct use of lance-rs crate (official implementation)
2. **Zero-Copy**: Arrow C Data Interface enables zero-copy data transfer
3. **Type-Safe**: Rust memory safety guarantees
4. **Future-Proof**: Lance development primarily in Rust

**Alternative Considered**: Python subprocess approach
- **Pros**: Easier to implement
- **Cons**: Process spawning overhead, data serialization overhead
- **Rejected**: Performance impact unacceptable for benchmarking

### Why Arrow C Data Interface

1. **Standard**: Apache Arrow official specification
2. **Zero-Copy**: No data serialization/deserialization
3. **Well-Defined**: Clear semantics for memory ownership
4. **Multilingual**: Works with any language (C++, Rust, Python, etc.)

---

## Notes for Future Maintainers

### Paimon Table Format Limitations

The current implementation:
- ✅ Supports append-only tables
- ❌ Does NOT support partitioning
- ❌ Does NOT support transactions
- ❌ Does NOT use paimon-cpp C++ library

For full Paimon feature support, would need to:
1. Integrate paimon-cpp C++ library
2. Implement partition discovery
3. Implement transaction management
4. Use Paimon-native metadata writers

Current implementation is sufficient for:
- Benchmarking basic table generation
- Testing data compatibility
- Performance profiling

### Lance Dataset Compatibility

Once Phase 2 is complete, Lance datasets will be readable by:
- Python: `import lance; ds = lance.dataset("path/to/dataset")`
- Spark: Via Spark-Lance connector
- DuckDB: Via Lance extension
- Flink: Via Flink-Lance connector

---

## References

### Paimon
- [Apache Paimon Documentation](https://paimon.apache.org/)
- [Paimon Table Format Specification](https://paimon.apache.org/docs/current/concepts/architecture/)
- [paimon-cpp GitHub](https://github.com/alibaba/paimon-cpp)

### Lance
- [Lance Format](https://github.com/lancedb/lance)
- [Lance v2 Specification](https://blog.lancedb.com/lance-v2/)
- [Lance Rust Crate](https://crates.io/crates/lance)

### Arrow
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
- [Arrow Parquet](https://arrow.apache.org/docs/python/parquet.html)
- [Arrow FFI Guide](https://arrow.apache.org/docs/format/CDataInterface/)

### Build System
- [CMake Best Practices](https://cliutils.gitlab.io/modern-cmake/)
- [Rust FFI](https://doc.rust-lang.org/nomicon/ffi.html)
- [Cargo Build Scripts](https://doc.rust-lang.org/cargo/build-script-examples/)

---

## Document History

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-31 | 1.0 | Initial plan: Phase 1 complete, Phase 2 pending |

---

**Next Action**: Await approval to proceed with Phase 2 (Lance FFI implementation)
