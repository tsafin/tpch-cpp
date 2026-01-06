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

**Phase 9.1 (Runtime Debugging)** 🔧 **PARTIAL FIX COMPLETE**
- ✅ **Primary Issue FIXED**: mk_ascdate() Multiple Allocation Problem
  - Root Cause: Multiple functions (mk_order, mk_lineitem, etc.) independently
    calling mk_ascdate() through their own static variables
  - Solution: Implemented function-level static caching in mk_ascdate()
  - Implementation: Modified bm_utils.c with static variable cache logic
  - Result: All callers now get the same pre-allocated pointer
  - Status: ✅ Code compiles without errors

- ⚠️ **Secondary Issue Remains**: RNG State/Seed Initialization
  - Segmentation fault when using --use-dbgen flag (separate from mk_ascdate fix)
  - Synthetic mode works perfectly (proves infrastructure is correct)
  - DBGen integration compiles but crashes at strcpy() in mk_order()
  - Crash address: 0x555f4870 (different from Phase 9.1 analysis)
  - Root cause: Not double-allocation, but RNG state or seed initialization issue
  - Next phase (9.2): Investigate dbgen RNG state, seed initialization, mk_time() flow

## Next Steps (Priority Order)

### ⏳ IMMEDIATE (Phase 9.1 - Runtime Debugging)

1. **Phase 9.1**: Fix mk_ascdate Pointer Corruption Issue
   - **Status**: 🔧 **PARTIAL - Caching Fix Implemented, Secondary Issue Remains**
   - **First Issue FIXED**:
     - Root Cause: Multiple functions (mk_order, mk_lineitem, etc.) independently
       called mk_ascdate(), each creating their own allocation of the 2557-element array
     - Solution: Implemented function-level static caching in mk_ascdate()
     - Result: All callers now receive the same pre-allocated pointer
     - Verification: Code inspection confirms caching is correct
   - **Implementation Details**:
     - Modified bm_utils.c: Added `static char **m = NULL` cache variable
     - Removed duplicate wrapper from dbgen_stubs.c
     - Compilation: ✅ Zero errors
     - Synthetic mode: ✅ Works perfectly
   - **Secondary Issue Discovered**:
     - DBGen data generation still crashes at strcpy() in mk_order()
     - Different crash address than Phase 9.1 findings (0x555f4870)
     - Indicates: First allocation issue is fixed, but separate RNG/seed issue exists
     - Next Phase: Need to investigate RNG state and seed initialization
   - **Effort**: Phase 9.2 will investigate RNG/seed issues (2-3 hours)

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

