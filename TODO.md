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

## Current Status (Updated Jan 2, 2026)

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

## Next Steps (Priority Order)

1. **Phase 9**: Scale Factor Support & Real DBGen Integration
   - Resolve dbgen C library build dependencies (text generation data)
   - Implement SF parameter support in dbgen_wrapper
   - Test with SF=1, 10, 100
   - Verify row count scaling matches TPC-H spec

2. **Phase 10**: Multi-Table Support
   - Add support for all 8 TPC-H tables beyond lineitem
   - orders, customer, part, partsupp, supplier, nation, region
   - Update main benchmark driver for table selection

3. **Phase 11**: Multi-Threading & Optimization
   - Multi-threaded data generation
   - Parallel table generation
   - Performance profiling and optimization

4. Comprehensive benchmarking harness (benchmark.sh - already done)
5. Integration with csbench for performance tracking

