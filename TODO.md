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

## Current Status

**Phase 8**: ORC support made explicit via `TPCH_ENABLE_ORC` CMake option
- Default build: Works with CSV and Parquet only
- Protobuf issues isolated and avoidable
- Full ORC support available when enabled

**Benchmarks Verified**:
- Parquet: 100 rows, 6804 bytes, 3333 rows/sec ✓
- CSV: 100 rows, 3219 bytes ✓
- Examples: simple_csv, simple_arrow_parquet, async_io_demo all working ✓

## Next Steps (Priority Order)

1. Resolve protobuf compatibility issues with ORC
2. Finalize dbgen integration (Phase 7.4+)
3. Add comprehensive benchmarking harness
4. Implement remaining TPC-H tables
5. Add multi-threaded data generation support

