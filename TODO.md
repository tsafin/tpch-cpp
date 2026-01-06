TODO
====

## Completed

1. ✅ Create simple C++ arrow writer example. Implement parquet format
   - Arrow schema creation and validation
   - Parquet file writing with Snappy compression
   - example: simple_arrow_parquet

2. ✅ Add csv support
   - CSV writer with RFC 4180 format
   - RFC 4180 compliant quoting and escaping
   - example: simple_csv

3. ⏸ Add orc support
   - ORC writer implemented and tested
   - **Disabled by default** due to protobuf compatibility issues
   - Use `-DTPCH_ENABLE_ORC=ON` to enable when ready
   - Will return to resolve protobuf conflicts

4. ✅ Add some asynchronous I/O support using io_uring and event processing
   - io_uring context implementation
   - async_io_demo example (production-quality)
   - Optional: `-DTPCH_ENABLE_ASYNC_IO=ON`

5. ✅ Import TPC-H dbgen
   - Official dbgen submodule integrated
   - DBGenWrapper C++ interface created (Phase 7)

6. 🚧 Connect orc and parquet writers to dbgen
   - Main driver infrastructure in place
   - Synthetic data generation working
   - Full dbgen integration deferred (Phase 7.4+)
   - Requires resolving multiple object file dependencies

## Current Status (Updated Jan 6, 2026)

**Phase 8.1**: ORC Runtime Issue Analysis & Documentation
- ORC rebuilt from source with system libraries using build_orc_from_source.sh
- Root cause clarified: Protobuf descriptor database collision (not ABI mismatch)
- Status: ✅ ORC builds with system libraries, CSV/Parquet work perfectly
- Issue: ORC + Arrow together cause protobuf duplicate registration error
- Workarounds documented for future resolution

**Phase 8.2**: dbgen Library Building Complete
- ✅ dbgen Makefile integration fixed and working
- ✅ libdbgen.a built successfully (178KB with all data generation objects)
- ✅ Object files compiled: build.o, bm_utils.o, rnd.o, print.o, etc.
- ✅ Ready for C++ wrapper integration

**Phase 8.3**: DBgen Integration Framework - CLI & Main Driver Support ✅ **COMPLETE**
- ✅ Added `--use-dbgen` flag to tpch_benchmark CLI
- ✅ Updated main.cpp with data generation framework
- ✅ Built with TPC-H-compliant synthetic data generator
- ✅ Project builds cleanly without dbgen library dependency
- ✅ Tested successfully: 250K rows/sec, 9.15 MB/sec write rate
- **Decision**: Use synthetic data for now, real dbgen integration deferred to Phase 9

**Benchmarks Verified**:
- Parquet: 500 rows, 19192 bytes, 250000 rows/sec ✓
- CSV: 100 rows, 3219 bytes ✓
- Examples: simple_csv, simple_arrow_parquet, async_io_demo all working ✓

**Phase 9**: Real DBGen Integration & Scale Factor Support ✅ **IMPLEMENTATION COMPLETE**
- ✅ Comprehensive planning documentation created (2,333 lines, 88 KB across 6 documents)
- ✅ PHASE_9_README.md - Navigation hub and reading guide
- ✅ PHASE_9_SUMMARY.md - Executive summary with key insights
- ✅ PHASE_9_PLAN.md - Complete technical specification (7-step plan)
- ✅ INTEGRATION_ARCHITECTURE.md - System design with diagrams and call sequences
- ✅ QUICK_REFERENCE.md - Step-by-step implementation guide with code snippets
- ✅ PLANNING_DELIVERABLES.md - Documentation manifest and index

**Phase 9 Implementation (Compilation)** ✅ **COMPLETE** (Jan 7, 2026)
- ✅ Created `include/tpch/dbgen_converter.hpp` (85 lines)
  - 8 converter functions (one per TPC-H table)
  - Generic dispatcher based on table name
- ✅ Created `src/dbgen/dbgen_converter.cpp` (267 lines)
  - Full implementations converting dbgen C structs to Arrow builders
  - Handles all 8 TPC-H tables with proper type conversions
  - String handling with length fields (clen)
  - Numeric conversions (divide by 100 for prices/quantities)
- ✅ Updated CMakeLists.txt
  - Enabled dbgen_wrapper.cpp and dbgen_converter.cpp
  - Added proper dbgen library configuration with EMBEDDED_DBGEN flag
  - Included necessary dbgen sources
- ✅ Integrated with src/main.cpp (already prepared in Phase 8.3)
  - Schema routing for all 8 tables
  - Callback-based data generation
  - Table selection via --table CLI flag
- ✅ Fixed dbgen embedding issues
  - Modified third_party/tpch/dbgen/tpch_dbgen.h for C-only macros
  - Modified third_party/tpch/dbgen/rnd.c with EMBEDDED_DBGEN guards
  - Created src/dbgen/dbgen_stubs.c with required globals
- ✅ Project builds successfully
  - Zero compilation errors
  - Synthetic data mode: ✅ WORKING (generates valid Parquet files)
  - Examples all build: simple_csv, simple_arrow_parquet, async_io_demo
  - Main tpch_benchmark executable builds without errors

**Phase 9.1 (Runtime Debugging)** ⚠️ **IN PROGRESS**
- ⚠️ Segmentation fault when using --use-dbgen flag
  - Synthetic mode works perfectly
  - dbgen integration compiles but crashes at runtime
  - Root cause identified: mk_ascdate() returns corrupted 32-bit pointers
  - Investigation findings:
    * Added dbgen_reset_seeds() calls to all table generation functions
    * Added mk_ascdate() initialization in init_dbgen()
    * Discovered mk_ascdate() returns array with corrupted 64-bit pointers
    * Raw bytes show: [32-bit ptr][ff ff ff ff] pattern - suggests 32-bit/64-bit issue
    * mk_order() calls strcpy(buffer, asc_date[index]) with invalid pointer
  - Next steps: Fix pointer size issue or use alternative date generation approach

## Next Steps (Priority Order)

### ⏳ IMMEDIATE (Phase 9.1 - Runtime Debugging)

1. **Phase 9.1**: Fix mk_ascdate Pointer Corruption Issue
   - **Status**: ✅ Root cause identified
   - **Problem**: mk_ascdate() returns array with corrupted pointers
     - Expected: array of char* (64-bit pointers on x86_64)
     - Actual: array with pattern [32-bit addr][0xffffffff...]
     - Symptom: strcpy(dest, corrupted_ptr) segfaults
   - **Suspected Cause**:
     - strdup() in bm_utils.c returning 32-bit values
     - Possible pointer size mismatch in C code compilation
     - Or malloc corruption in embedded mode
   - **Solution Options** (Prioritized):
     1. Check if bm_utils.c has size_t or pointer issues with embedded mode
     2. Create wrapper for mk_ascdate that validates/fixes pointers
     3. Implement alternative date string generation (don't use mk_ascdate)
     4. Use synthetic dates instead of calling dbgen's mk_* functions
   - **Effort**: 2-3 hours for full investigation and fix

### 🚀 FOLLOW-UP (Phase 10+)

2. **Phase 10**: Distributed/Parallel Generation
   - Multi-threaded table generation (one per thread)
   - Parallel scale factor support
   - Process-based parallelization
   - Estimated effort: 3-4 hours

3. **Phase 11**: Performance Optimization
   - SIMD optimizations for row appending
   - Parallel I/O operations
   - Memory pool optimization
   - Estimated effort: 4-5 hours

4. **Phase 12**: Query Integration
   - DuckDB/Polars integration
   - Full end-to-end TPC-H benchmarking
   - Report generation
   - Estimated effort: 5-6 hours

### 📚 REFERENCE DOCUMENTS

All Phase 9 planning documents are in `/home/tsafin/src/tpch-cpp/`:
- PHASE_9_README.md - Start here for navigation
- PHASE_9_SUMMARY.md - 5-minute overview
- PHASE_9_PLAN.md - Full technical specification
- QUICK_REFERENCE.md - Implementation guide with code
- INTEGRATION_ARCHITECTURE.md - System design details
- PLANNING_DELIVERABLES.md - Documentation manifest

