# Phase 9: Real DBGen Integration & Scale Factor Support

## Overview

This phase connects the official TPC-H dbgen C library with the tpch-cpp project's C++ writer infrastructure. The goal is to replace synthetic data generation with authentic TPC-H data produced by the reference dbgen implementation.

**Status**: Planning
**Parent Issue**: Phase 8.3 deferred dbgen integration
**Dependencies**: Phase 8.2 (libdbgen.a build) completed successfully

---

## Current State Analysis

### What Works ✅
- **dbgen_wrapper.hpp/cpp**: Complete C++ wrapper around dbgen C code
  - Full API for all 8 TPC-H tables (lineitem, orders, customer, part, partsupp, supplier, nation, region)
  - Iterator-style callbacks for row-by-row generation
  - Arrow schema definitions for all tables
  - Proper scale factor support via `get_row_count()` function
  - Safe initialization with move semantics

- **libdbgen.a**: Successfully built (Phase 8.2)
  - All required object files compiled: build.o, bm_utils.o, rnd.o, print.o, etc.
  - Linked as static archive (178 KB)
  - Ready for C++ wrapper integration

- **main.cpp / CLI**: Infrastructure exists
  - `--use-dbgen` flag already defined
  - `--scale-factor` parameter parsed
  - Options struct prepared for extension
  - Writer abstraction supports multiple formats

- **CMakeLists.txt**: Build system ready
  - Links `dbgen_lib` target (line 126)
  - Conditional ORC support
  - Third-party/dbgen subdirectory integrated

### What's Missing ❌
- **Actual dbgen integration in main.cpp**: Currently deferred (line 199-200, 213-277)
  - Synthetic data loop instead of calling DBGenWrapper
  - No callback infrastructure to populate Arrow builders
  - No multi-table support in benchmark driver

- **Builder/Converter Infrastructure**: Not yet implemented
  - Mechanism to convert C dbgen structs → Arrow arrays
  - Batch accumulation logic (collect rows in builders, flush to Arrow RecordBatch)
  - Proper null handling and type conversion
  - Row limit support with current row tracking

- **DBGenWrapper linking**: Currently commented out in CMakeLists.txt (line 108)
  - Source file not included in TPCH_CORE_SOURCES
  - Needs to be uncommented after dbgen build is verified

- **Error Handling**: Basic exception handling exists but needs:
  - dbgen C function error codes interpretation
  - Recovery from generation failures

---

## Architecture: How dbgen_wrapper Connects to Writers

### Data Flow

```
DBGenWrapper::generate_lineitem()
    │
    ├─ Calls row_start(DBGEN_TABLE_LINE)
    ├─ For each order index i:
    │   ├─ Calls mk_order(i, &order, 0)  ← dbgen C function
    │   ├─ Iterates over order.lines (1-7 items per order)
    │   └─ Invokes callback(&order.l[j])  ← passes each lineitem row
    │
    └─ Calls row_stop(DBGEN_TABLE_LINE)

Callback in main.cpp:
    │
    └─ Receives void* pointer (actually line_t*)
        ├─ Cast to line_t*
        ├─ Extract fields: l_orderkey, l_partkey, etc.
        └─ Append to Arrow builders
```

### Key Design Pattern: Iterator via Callbacks

The dbgen_wrapper uses **callback-based iteration** (function pointers, C++11 lambdas):
- `generate_lineitem()` takes `std::function<void(const void*)> callback`
- For each generated row, callback is invoked with pointer to C struct
- Caller (main.cpp) receives row and can:
  - Buffer in Arrow builders
  - Accumulate in batches
  - Flush when batch size reached

**Why this approach?**
1. Avoids dbgen global state issues (dbgen uses global variables)
2. Allows batch-oriented processing (collect N rows, then flush)
3. Non-intrusive integration (dbgen.c/h unchanged)
4. Efficient (single pass through data)

---

## Implementation Steps (Phase 9)

### Step 1: Enable DBGenWrapper Compilation
**File**: `CMakeLists.txt` line 108
**Change**:
```cmake
# Before:
    # src/dbgen/dbgen_wrapper.cpp  # Deferred - dbgen integration pending

# After:
    src/dbgen/dbgen_wrapper.cpp
```

**Why**: Make wrapper objects available for linking in main.cpp

**Verification**:
```bash
cmake -B build && make -B
# Should compile dbgen_wrapper.cpp without errors
```

### Step 2: Create Arrow Builder Converter Utilities
**New File**: `include/tpch/dbgen_converter.hpp`

**Purpose**: Helper functions to convert C dbgen structs → Arrow arrays

**API** (example for lineitem):
```cpp
namespace tpch {
  /**
   * Append a dbgen line_t struct to Arrow builders
   *
   * @param row Pointer to line_t (const void* from callback)
   * @param builders Map of column_name -> Arrow builder
   */
  void append_lineitem_to_builders(
      const void* row,
      std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

  // Similar functions for other tables:
  // - append_orders_to_builders()
  // - append_customer_to_builders()
  // - append_part_to_builders()
  // - append_partsupp_to_builders()
  // - append_supplier_to_builders()
  // - append_nation_to_builders()
  // - append_region_to_builders()
}
```

**Implementation Pattern** (lineitem example):
```cpp
void append_lineitem_to_builders(const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {
  auto* line = static_cast<const line_t*>(row);

  // Append to each builder
  std::get<arrow::Int64Builder*>(builders["l_orderkey"])->Append(line->okey);
  std::get<arrow::Int64Builder*>(builders["l_partkey"])->Append(line->partkey);
  // ... etc for each field
}
```

**Files to modify**:
- Create: `include/tpch/dbgen_converter.hpp`
- Create: `src/dbgen/dbgen_converter.cpp`
- Update: `CMakeLists.txt` (add to TPCH_CORE_SOURCES)

### Step 3: Refactor main.cpp to Use DBGenWrapper
**File**: `src/main.cpp`

**Changes**:
1. Add include:
   ```cpp
   #include "tpch/dbgen_wrapper.hpp"
   #include "tpch/dbgen_converter.hpp"
   ```

2. Add to Options struct (line 23-31):
   ```cpp
   struct Options {
       // ... existing fields ...
       bool use_dbgen = false;  // Already exists
       std::string table = "lineitem";  // NEW: which table to generate
   };
   ```

3. Add CLI option parsing for `--table`:
   ```cpp
   {"table", required_argument, nullptr, 't'},
   // Case 't': opts.table = optarg;
   ```

4. Replace synthetic data loop (lines 199-277) with dbgen logic:

   **Pseudocode**:
   ```cpp
   if (opts.use_dbgen) {
       // Create wrapper with scale factor
       tpch::DBGenWrapper dbgen(opts.scale_factor);

       // Generate appropriate table based on opts.table
       if (opts.table == "lineitem") {
           generate_with_dbgen(dbgen, opts, schema, writer,
                               &tpch::DBGenWrapper::generate_lineitem);
       } else if (opts.table == "orders") {
           // ... etc for other tables
       }
   } else {
       // Use existing synthetic data (keep current code)
   }
   ```

5. New helper function: `generate_with_dbgen()`
   ```cpp
   void generate_with_dbgen(
       tpch::DBGenWrapper& dbgen,
       const Options& opts,
       std::shared_ptr<arrow::Schema> schema,
       std::unique_ptr<tpch::WriterInterface>& writer,
       std::function<void(tpch::DBGenWrapper&,
                          std::function<void(const void*)>)> generate_fn) {

       const size_t batch_size = 10000;
       size_t total_rows = 0;
       size_t rows_in_batch = 0;

       // Create builders from schema
       auto builders = create_builders_from_schema(schema);

       // Lambda callback for dbgen
       auto append_callback = [&](const void* row) {
           // Append row to builders using converter
           append_row_to_builders(opts.table, row, builders);
           rows_in_batch++;
           total_rows++;

           // Flush when batch full or at end
           if (rows_in_batch >= batch_size) {
               auto batch = finish_batch(schema, builders, rows_in_batch);
               writer->write_batch(batch);
               reset_builders(builders);
               rows_in_batch = 0;
           }
       };

       // Generate rows via callback
       generate_fn(dbgen, append_callback);

       // Flush any remaining rows
       if (rows_in_batch > 0) {
           auto batch = finish_batch(schema, builders, rows_in_batch);
           writer->write_batch(batch);
       }
   }
   ```

### Step 4: Test Integration with Lineitem
**Execution**:
```bash
# Build with dbgen enabled
cd /home/tsafin/src/tpch-cpp
rm -rf build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# Test with scale factor 1, lineitem table
./tpch_benchmark \
  --use-dbgen \
  --scale-factor 1 \
  --table lineitem \
  --format parquet \
  --output-dir /tmp \
  --verbose
```

**Expected Output**:
- 6,000,000 lineitem rows for SF=1 (per TPC-H spec)
- Execution time: ~30-60 seconds (1-2 million rows/sec)
- Valid Parquet file readable by pyarrow

**Verification**:
```bash
# Verify with Python
python3 << 'EOF'
import pyarrow.parquet as pq
table = pq.read_table("/tmp/sample_data.parquet")
print(f"Rows: {table.num_rows}")
print(f"Schema: {table.schema}")
# Should show 6,000,000 rows with correct schema
EOF
```

### Step 5: Implement Multi-Table Support
**Files**: `src/main.cpp`

**Changes**:
1. Update Options for table selection:
   ```cpp
   std::string table = "lineitem";  // Default
   ```

2. Add table validation:
   ```cpp
   const std::vector<std::string> valid_tables = {
       "lineitem", "orders", "customer", "part",
       "partsupp", "supplier", "nation", "region"
   };
   if (std::find(valid_tables.begin(), valid_tables.end(), opts.table)
       == valid_tables.end()) {
       std::cerr << "Invalid table: " << opts.table << "\n";
       return 1;
   }
   ```

3. Implement table schema selection:
   ```cpp
   std::shared_ptr<arrow::Schema> schema;
   if (opts.table == "lineitem") {
       schema = tpch::DBGenWrapper::get_schema(tpch::TableType::LINEITEM);
   } else if (opts.table == "orders") {
       schema = tpch::DBGenWrapper::get_schema(tpch::TableType::ORDERS);
   }
   // ... etc for other tables
   ```

4. Implement table generation dispatch:
   ```cpp
   if (opts.table == "lineitem") {
       generate_with_dbgen(dbgen, opts, schema, writer,
           [](auto& g, auto& cb) { g.generate_lineitem(cb); });
   } else if (opts.table == "orders") {
       generate_with_dbgen(dbgen, opts, schema, writer,
           [](auto& g, auto& cb) { g.generate_orders(cb); });
   }
   // ... etc
   ```

### Step 6: Add Row Limit Support
**File**: `src/main.cpp`

**Changes**:
- Pass `opts.max_rows` to generate functions:
  ```cpp
  dbgen.generate_lineitem(append_callback, opts.max_rows);
  ```
- Already supported in dbgen_wrapper.cpp (max_rows parameter)

### Step 7: Validate Output Correctness
**Test Matrix**:
```
Scale Factors: 1, 10
Tables: lineitem, orders, customer (at minimum)
Formats: parquet, csv (ORC optional)
Checks:
  - Row counts match TPC-H spec: get_row_count()
  - Data types correct
  - No nulls in primary keys
  - Sample data values in expected ranges
```

**Verification Script** (`scripts/verify_dbgen_output.py`):
```python
import pyarrow.parquet as pq
import sys

def verify_lineitem(parquet_file, sf):
    table = pq.read_table(parquet_file)
    expected_rows = 6_000_000 * sf

    if table.num_rows != expected_rows:
        print(f"ERROR: Expected {expected_rows} rows, got {table.num_rows}")
        return False

    # Check schema
    schema = table.schema
    required_cols = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_quantity']
    for col in required_cols:
        if col not in schema.names:
            print(f"ERROR: Missing column {col}")
            return False

    # Check for nulls in keys
    if table['l_orderkey'].null_count > 0:
        print("ERROR: l_orderkey has nulls")
        return False

    print(f"✓ Lineitem verified: {table.num_rows} rows")
    return True

# Usage
if __name__ == "__main__":
    parquet_file = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sample_data.parquet"
    sf = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    verify_lineitem(parquet_file, sf)
```

---

## Converter Implementation Details

### Lineitem Converter (Example Pattern for All Tables)

The `line_t` struct from dbgen:
```c
typedef struct {
    int64_t okey;
    int64_t partkey;
    int64_t suppkey;
    int64_t lcnt;                    // line count (1-7)
    int64_t quantity;                // stored as integer in dbgen
    int64_t eprice;                  // extended price
    int64_t discount;
    int64_t tax;
    char rflag[1];                   // "R", "A", "N"
    char lstatus[1];                 // "O", "F"
    char cdate[DATE_LEN];            // YYYY-MM-DD format
    char sdate[DATE_LEN];
    char rdate[DATE_LEN];
    char shipinstruct[11];           // "NONE", "TRUCK", etc
    char shipmode[11];               // "MAIL", "AIR", etc
    char comment[L_CMNT_MAX + 1];    // up to 44 chars
    int clen;                        // comment length
} line_t;
```

### Type Conversions Required
| C Type | Arrow Type | Notes |
|--------|-----------|-------|
| `int64_t` | `int64` | Direct |
| `char[N]` | `utf8` | Convert to null-terminated string, strip excess |
| `quantity` (stored as int) | `float64` | Divide by 100 (dbgen stores as cents) |
| `eprice`, `discount`, `tax` | `float64` | Already scaled correctly |

### Null Handling
- Primary keys (orderkey, partkey, etc.): Never null
- Strings: Terminate at clen boundary or `\0`, whichever comes first
- Numbers: dbgen never generates nulls for required fields

---

## Testing Strategy

### Phase 9.1: Basic Connectivity
- [ ] Compile dbgen_wrapper.cpp without errors
- [ ] Link tpch_core library with libdbgen.a
- [ ] Create dbgen_converter.hpp with minimal implementation
- [ ] Test callback mechanism with simple row counting

### Phase 9.2: Lineitem Integration
- [ ] Implement append_lineitem_to_builders()
- [ ] Modify main.cpp to use DBGenWrapper for lineitem
- [ ] Generate SF=1 lineitem (6M rows)
- [ ] Verify row count = 6,000,000
- [ ] Validate with pyarrow

### Phase 9.3: Multi-Table Support
- [ ] Implement converters for remaining 7 tables
- [ ] Add table selection CLI option
- [ ] Test each table at SF=1
- [ ] Verify row counts match spec

### Phase 9.4: Scale Factor Testing
- [ ] Test SF=1, 10, 100 for lineitem
- [ ] Verify row counts scale linearly
- [ ] Performance profiling

### Phase 9.5: Format Support
- [ ] Generate all tables in CSV format
- [ ] Generate all tables in Parquet format
- [ ] ORC format (if Phase 8.1 issue resolved)

---

## Rollback Plan

If dbgen integration encounters critical issues:
1. Keep `--use-dbgen` flag functional but default to synthetic data
2. Commit dbgen_wrapper as-is for future phases
3. Document blockers in TODO.md
4. Continue with synthetic data path

---

## Estimated Complexity

**New Code**: ~500-700 LOC
- dbgen_converter.hpp/cpp: ~200 LOC
- main.cpp modifications: ~300-400 LOC
- Helper functions and utilities: ~100 LOC

**Build Changes**: ~20 LOC
- CMakeLists.txt modifications

**Testing/Validation**: ~50 LOC (verification scripts)

---

## Success Criteria

✅ All 8 TPC-H tables can be generated using official dbgen
✅ Row counts match TPC-H specification for SF=1,10,100
✅ Output files (Parquet, CSV) are valid and readable
✅ Performance: ≥1M rows/sec for lineitem
✅ Zero data corruption (validated via schema and key checks)
✅ `--use-dbgen` flag works reliably
✅ Backward compatibility maintained (synthetic data still works)

---

## Dependencies & Blockers

**Hard Dependencies**:
- ✅ libdbgen.a successfully built (Phase 8.2)
- ✅ dbgen_wrapper.hpp/cpp complete (Phase 7.4)
- ✅ Arrow/Parquet working (Phases 1-3)

**Soft Dependencies**:
- ORC integration (Phase 8.1 - optional)
- Performance targets (Phase 11)

**No Blockers Identified**: Ready to proceed

---

## Next Steps After Phase 9

- **Phase 10**: Distributed/parallel generation (multiple scale factors simultaneously)
- **Phase 11**: Performance optimization (SIMD row appending, parallel I/O)
- **Phase 12**: Direct query integration (DuckDB/Polars ingestion)

