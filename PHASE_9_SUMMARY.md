# Phase 9: Executive Summary

**Status**: Planning Complete ✅
**Date**: January 6, 2026
**Scope**: Real DBGen Integration & Scale Factor Support
**Effort**: ~500-700 lines of new code across 4 files

---

## What We're Building

Connecting the official TPC-H dbgen C library with tpch-cpp's C++ writer infrastructure to generate authentic TPC-H benchmark data.

```
Before (Phase 8): Synthetic data
                  tpch_benchmark → hardcoded fake lineitem rows → Parquet/CSV

After (Phase 9):  Real TPC-H data
                  tpch_benchmark → DBGenWrapper → real dbgen → Parquet/CSV
```

---

## Key Insight: The Callback Pattern

Instead of trying to integrate dbgen into our data flow directly, we use **callbacks**:

```cpp
DBGenWrapper dbgen(scale_factor);

dbgen.generate_lineitem([&](const void* row) {
    // This lambda is called 6M times for lineitem at SF=1
    // Each call receives a C struct pointer (line_t*)
    append_row_to_builders(row, builders);  // Convert to Arrow
});
```

**Why this works**:
- dbgen already generates all the data correctly
- We just need to capture each row as it's generated
- Lambda callbacks let us plug in our Arrow conversion logic
- Single-pass, efficient, no global state conflicts

---

## Implementation Overview

### New Files (2)
1. **`include/tpch/dbgen_converter.hpp`** - 80 lines
   - 8 converter functions (one per table)
   - Convert C structs → Arrow builders

2. **`src/dbgen/dbgen_converter.cpp`** - 400 lines
   - Implementation of converters
   - Field extraction and type conversion logic

### Modified Files (2)
1. **`CMakeLists.txt`** - 2 line change
   - Uncomment dbgen_wrapper.cpp compilation
   - Add dbgen_converter.cpp to build

2. **`src/main.cpp`** - ~300 line change
   - Add --table CLI option
   - Implement generate_with_dbgen() helper
   - Replace synthetic loop with dbgen dispatch
   - Multi-table schema/generation routing

### Unchanged (Why We Can Reuse Existing Code)
- dbgen_wrapper.hpp/cpp (already complete!)
- All writers (Parquet, CSV, ORC)
- CMake modules
- libdbgen.a (Phase 8.2 completed)

---

## The Data Flow

```
Command:
  ./tpch_benchmark --use-dbgen --scale-factor 1 --table lineitem

⬇️

main.cpp parses args:
  - use_dbgen = true
  - scale_factor = 1
  - table = "lineitem"

⬇️

Create schema from DBGenWrapper::get_schema(LINEITEM)
  - 16 Arrow fields (l_orderkey, l_partkey, ..., l_comment)

⬇️

Create DBGenWrapper(1)
  - Prepares to generate TPC-H data at SF=1

⬇️

generate_with_dbgen():
  - Create Arrow builders (one per column)
  - Define append_callback lambda
  - Call dbgen.generate_lineitem(append_callback)

⬇️

For each order (1 to 1.5M):
  - dbgen's mk_order() generates order struct
  - For each lineitem in order (1-7 items):
    - append_callback(&lineitem_struct) invoked

⬇️

Inside callback (dbgen_converter.cpp):
  - Cast void* to line_t*
  - Extract l_orderkey → append to Int64Builder
  - Extract l_partkey → append to Int64Builder
  - Extract l_quantity → convert to double, append
  - ... all 16 fields ...
  - Increment row counter

⬇️

When batch_size reached (10K rows):
  - finish_batch() converts builders to Arrow RecordBatch
  - writer->write_batch(batch) writes to Parquet/CSV
  - reset_builders() clears for next batch
  - Repeat

⬇️

After all 6M rows:
  - Flush remaining rows
  - Close writer
  - Print summary (6M rows, 5 seconds, 1.2M rows/sec)

⬇️

Output: /tmp/sample_data.parquet
  Valid Parquet file readable by pyarrow, Spark, DuckDB, etc.
```

---

## What Exists & What Doesn't

### ✅ Already Complete (Previous Phases)
- **dbgen_wrapper.hpp/cpp**: Full C++ wrapper with all 8 tables
- **libdbgen.a**: Compiled and linked successfully
- **Arrow/Parquet support**: Working writers and schemas
- **CLI infrastructure**: Options parsing, output handling
- **Schema definitions**: All TPC-H table schemas defined

### ❌ Missing (Phase 9 Only)
- Converter functions (C struct → Arrow builders)
- Main.cpp integration logic
- Multi-table dispatch
- Schema routing logic

### 🔄 Reused (Existing Infrastructure)
- WriterInterface abstraction
- CMake build system
- Arrow library integration
- dbgen_wrapper callback API

---

## Scale Factor Support

The wrapper already supports scale factors via TPC-H formulas:

```cpp
get_row_count(TableType::LINEITEM, 1)    // 6,000,000 rows
get_row_count(TableType::LINEITEM, 10)   // 60,000,000 rows
get_row_count(TableType::LINEITEM, 100)  // 600,000,000 rows
```

Our job: Pass scale_factor to DBGenWrapper(sf) and let it do the work.

---

## Why This Approach Is Superior

| Aspect | Synthetic (Phase 8) | Real dbgen (Phase 9) |
|--------|---------------------|---------------------|
| **Correctness** | Hardcoded values | Official TPC-H spec |
| **Scale** | Limited | Arbitrary (SF=1 to 1000+) |
| **Reproducibility** | Inconsistent | Deterministic seed |
| **Query realism** | Low | High (real TPC-H data) |
| **Performance testing** | Not valid | Valid for benchmarking |
| **Compliance** | Not TPC-H compliant | TPC-H compliant |

For any serious benchmarking or compliance testing, synthetic data is unsuitable. Phase 9 bridges this gap.

---

## Implementation Risk Assessment

**Low Risk**: This is straightforward integration work
- No novel algorithms
- All pieces exist and work independently
- Clear data flow
- Easy to test incrementally

**Potential Issues & Mitigations**:
| Issue | Likelihood | Mitigation |
|-------|------------|-----------|
| Type conversion errors | Medium | Verify with Python pyarrow |
| String truncation | Low | Use clen for comment fields |
| Null handling | Low | dbgen doesn't generate nulls |
| Builder reset issues | Low | Verify batch boundaries |
| Scale factor setup | Low | Test with SF=1,10 first |

---

## Testing & Validation Strategy

### Phase 9.1: Connectivity
- [ ] Compile dbgen_wrapper.cpp successfully
- [ ] Link with libdbgen.a
- [ ] Callback mechanism works (simple row counter)

### Phase 9.2: Lineitem
- [ ] Generate 6M lineitem rows (SF=1)
- [ ] Verify row count with pyarrow
- [ ] Spot check: l_orderkey range, l_quantity values

### Phase 9.3: All Tables
- [ ] Generate each table at SF=1
- [ ] Verify row counts match spec
- [ ] Validate schemas

### Phase 9.4: Scale Factors
- [ ] SF=1, SF=10, SF=100
- [ ] Verify row counts scale linearly

### Phase 9.5: Format Support
- [ ] CSV output for all tables
- [ ] Parquet output for all tables
- [ ] ORC (if Phase 8.1 resolved)

---

## Performance Expectations

For lineitem at SF=1 (6M rows):

```
Phase 8 (Synthetic):  0.02 seconds (fake data, trivial)
Phase 9 (Real dbgen): 5-10 seconds (real data, dbgen overhead)

Breakdown:
- dbgen row generation:  2-3 seconds (intrinsic)
- Arrow builder append:  1-2 seconds (our code)
- Parquet write:         2-3 seconds (I/O bound)
```

Target: **≥1M rows/sec** for lineitem, which translates to:
- ~6 seconds total for 6M rows
- ~1M rows/second throughput

This is acceptable for benchmarking (not as fast as synthetic, but legitimate TPC-H data).

---

## Integration Points

```
User Command
    ↓
main.cpp: parse_args()
    ├─ --use-dbgen flag
    ├─ --scale-factor parameter
    ├─ --table selection
    ├─ --format choice
    └─ --output-dir
    ↓
Schema Selection
    └─ DBGenWrapper::get_schema(table_type)
    ↓
DBGenWrapper Initialization
    └─ DBGenWrapper(scale_factor)
    ↓
Data Generation
    └─ DBGenWrapper.generate_[table]([callback])
            ├─ C function calls (dbgen C library)
            └─ Callback for each row
    ↓
Callback: dbgen_converter.cpp
    └─ Append row to Arrow builders
    ↓
When batch full or done:
    └─ finish_batch() → RecordBatch
    ↓
Writer Interface
    └─ write_batch(recordbatch)
    ↓
Output Files
    └─ /tmp/sample_data.{parquet,csv,orc}
```

---

## Files Overview

### Documentation (What We Created)
1. **PHASE_9_PLAN.md** (This detailed plan)
   - 500+ lines of specification
   - Step-by-step implementation guide
   - Testing strategy
   - Success criteria

2. **INTEGRATION_ARCHITECTURE.md** (System design)
   - Data flow diagrams
   - Call sequences
   - Component interactions
   - C struct definitions

3. **QUICK_REFERENCE.md** (Copy-paste guide)
   - Code snippets for each file
   - Testing commands
   - Debugging tips
   - Performance notes

### Code to Write
1. **include/tpch/dbgen_converter.hpp** (80 lines)
   - Function declarations

2. **src/dbgen/dbgen_converter.cpp** (400 lines)
   - Implementation for 8 tables
   - Type conversions
   - Field extraction

3. **Modifications to src/main.cpp** (~300 lines)
   - Helper functions
   - Schema routing
   - Table dispatch
   - Data generation logic

4. **Modifications to CMakeLists.txt** (2 lines)
   - Uncomment dbgen_wrapper.cpp
   - Add dbgen_converter.cpp

---

## Timeline Estimate (Not Binding)

If implementing immediately:
- File creation + implementation: 2-3 hours
- Testing & debugging: 2-3 hours
- Validation & optimization: 1-2 hours
- Documentation updates: 1 hour
- **Total: 6-9 hours of work** for a single developer

However, given the clear specifications and reusable components, actual time may be less.

---

## Success Metrics

After Phase 9 is complete, these should all be true:

✅ `./tpch_benchmark --use-dbgen --scale-factor 1 --table lineitem` generates 6,000,000 rows
✅ Row counts match TPC-H spec for all 8 tables
✅ Output files are valid Parquet/CSV readable by standard tools
✅ No data corruption (primary keys unique, no nulls where unexpected)
✅ Performance ≥1M rows/sec for lineitem
✅ All scale factors (1, 10, 100) work correctly
✅ `--use-dbgen` flag is reliable and well-tested
✅ Backward compatibility maintained (synthetic mode still works)

---

## What Happens Next

### Immediate (Phase 9 Implementation)
1. Create converter functions
2. Integrate into main.cpp
3. Test each table
4. Validate output

### Short-term (Phase 10)
1. Distributed generation (multiple tables simultaneously)
2. Parallel data generation across CPU cores
3. Performance optimization

### Medium-term (Phase 11+)
1. Query integration (DuckDB/Polars)
2. Benchmarking harness improvements
3. Cross-platform support (Windows, macOS)
4. Advanced I/O optimizations

---

## Key References

**In This Repository**:
- `third_party/dbgen/` - Official TPC-H dbgen source
- `include/tpch/dbgen_wrapper.hpp` - Complete API reference
- `src/dbgen/dbgen_wrapper.cpp` - Implementation details

**External Resources**:
- TPC-H Specification (http://www.tpc.org/tpch)
- Apache Arrow Documentation
- Apache Parquet Format Specification

**Previous Phases**:
- Phase 7: DBGenWrapper creation
- Phase 8.2: libdbgen.a build
- Phase 8.3: CLI infrastructure

---

## Questions & Clarifications

**Q: Why use callbacks instead of iterators?**
A: dbgen uses global state for RNG seeds and other context. Callbacks give dbgen full control over iteration, avoiding re-entrancy issues.

**Q: Can we generate multiple tables in parallel?**
A: Not in Phase 9. Each DBGenWrapper instance uses global dbgen state, so they must be sequential. Phase 10 will address this with process-per-table or thread-safe wrapper refactoring.

**Q: What about NULL values in optional fields?**
A: dbgen doesn't generate NULLs in required fields, and optional fields are rare in TPC-H. We can add NULL support later if needed.

**Q: How do we handle very large scale factors (SF=1000)?**
A: Memory-efficiently through batching. Phase 9 uses 10K-row batches, keeping memory usage constant regardless of total rows.

**Q: Can we skip dbgen integration for now?**
A: Yes! The `--use-dbgen` flag is optional. Synthetic mode continues to work. But real benchmarking requires Phase 9.

---

## Conclusion

Phase 9 is a **straightforward integration** of existing, working components:
- dbgen_wrapper (complete) ✅
- libdbgen.a (compiled) ✅
- Arrow/Parquet support (working) ✅

We just need to:
1. Write 8 simple converter functions
2. Add table routing logic to main.cpp
3. Test each table

**No algorithmic complexity. No novel systems. Just integration.**

The plans document (PHASE_9_PLAN.md) contains step-by-step instructions for every change needed. The quick reference (QUICK_REFERENCE.md) provides ready-to-use code snippets.

---

## Next Action

1. Read PHASE_9_PLAN.md for detailed specifications
2. Read QUICK_REFERENCE.md for implementation code
3. Follow step-by-step instructions in both documents
4. Test iteratively (Phase 9.1 → 9.2 → ... → 9.5)
5. Validate output with provided Python scripts
6. Commit as "Phase 9: Real DBGen Integration & Scale Factor Support"

**All planning is complete. Ready to implement.** ✅

