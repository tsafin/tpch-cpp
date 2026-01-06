# DBGen Integration Architecture

## System Overview Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            tpch_benchmark                              │
│                          (src/main.cpp)                                │
│                                                                         │
│  Options:                                                              │
│  - --scale-factor 1      (from CLI)                                   │
│  - --use-dbgen           (enables dbgen mode)                         │
│  - --table lineitem      (NEW: which table)                           │
│  - --format parquet      (output format)                              │
│  - --output-dir /tmp                                                 │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ├─── if use_dbgen ───→ ┌──────────────────┐
                               │                      │  DBGenWrapper    │
                               │                      │  (new mode)      │
                               │                      └────────┬─────────┘
                               │                               │
                               │ else                          │
                               ├──→ Synthetic data             │ (existing code)
                               │    (current implementation)   │
                               │                               │
                               └──────────────────────────────┘
                                          │
                    ┌─────────────────────┴─────────────────────┐
                    │                                           │
            ┌───────▼──────────┐                         ┌──────▼──────────┐
            │  Arrow Builders  │                         │  DBGenWrapper   │
            │  (one per col)   │                         │   C++ Wrapper   │
            │                  │                         │                 │
            │ - l_orderkey     │                         │ generate_*()    │
            │ - l_partkey      │                         │ (callback-based)│
            │ - l_quantity     │                         │                 │
            │ - ... (10 cols)  │                         │ Supports:       │
            └───────┬──────────┘                         │ - lineitem      │
                    │                                    │ - orders        │
                    │                                    │ - customer      │
                    │ (accumulate rows)                  │ - part          │
                    │ (batch_size = 10K)                 │ - partsupp      │
                    │                                    │ - supplier      │
                    │                                    │ - nation        │
                    │                                    │ - region        │
                    │                                    └────────┬────────┘
                    │                                             │
                    │ ┌─ dbgen_converter ──────────────────────┐  │
                    │ │                                        │  │
                    │ │ Callback from generate_*()            │  │
                    │ │ └─→ void* row (cast to line_t*)       │  │
                    │ │     append_lineitem_to_builders()     │  │
                    │ │     ├─ line->okey → l_orderkey        │  │
                    │ │     ├─ line->partkey → l_partkey      │  │
                    │ │     ├─ line->quantity → l_quantity    │  │
                    │ │     ├─ ... (all fields)               │  │
                    │ │     └─ builders updated               │  │
                    │ └─────────────────────────────────────┘  │
                    │                                          │
                    └───────────────┬───────────────────────────┘
                                    │ (when batch_size reached)
                    ┌───────────────▼────────────────┐
                    │   Arrow RecordBatch            │
                    │   (10K rows)                   │
                    │                                │
                    │ [Int64, Int64, Float64, ...]  │
                    │ [6M rows processed]            │
                    └───────────────┬────────────────┘
                                    │
                    ┌───────────────▼────────────────┐
                    │  WriterInterface (Factory)     │
                    │                                │
                    │ create_writer("parquet",       │
                    │               path)            │
                    └───────┬─────────────────┬──────┘
                            │                 │
                    ┌───────▼─────┐   ┌───────▼─────┐
                    │ ParquetWriter│   │  CSVWriter  │
                    │  write_batch │   │ write_batch │
                    │  (.parquet)  │   │  (.csv)     │
                    └───────┬──────┘   └───────┬──────┘
                            │                 │
                            └────────┬────────┘
                                     │
                            ┌────────▼────────┐
                            │ /tmp/sample_data│
                            │ .parquet/.csv   │
                            └─────────────────┘
```

## Data Flow: Callback Pattern

```
main.cpp: generate_with_dbgen()
    │
    ├─ Create Arrow builders from schema
    │
    └─ Define append_callback = [&](const void* row) {
           append_row_to_builders(table, row, builders);
           rows_in_batch++;
           if (rows_in_batch >= 10000) flush_batch();
       }
           │
           └─ DBGenWrapper.generate_lineitem(append_callback)
                  │
                  └─ for (order i = 1..1.5M) {
                         mk_order(i, &order_struct, 0);  // C function
                         for (j = 0; j < order.lines; j++) {
                             append_callback(&order.l[j]);
                                 │
                                 └─ Cast line_t* and append fields
                                    to builders
                         }
                     }
                  │
                  └─ Returns when done
           │
           └─ Flush remaining rows
```

## Key Components

### 1. DBGenWrapper (Phase 7 - Complete)
**File**: `include/tpch/dbgen_wrapper.hpp`

```cpp
class DBGenWrapper {
    // Public API - all these generate data via callback
    void generate_lineitem(
        std::function<void(const void* row)> callback,
        long max_rows = -1);

    void generate_orders(...);
    void generate_customer(...);
    void generate_part(...);
    void generate_partsupp(...);
    void generate_supplier(...);
    void generate_nation();
    void generate_region();

    // Schema creation
    static std::shared_ptr<arrow::Schema> get_schema(TableType table);

    // Row count lookup
    static long get_row_count(TableType table, long scale_factor);
};
```

**Data Generation Mechanism**:
- Wraps official dbgen C code: `mk_order()`, `mk_part()`, `mk_cust()`, etc.
- Calls dbgen functions in loop
- For each generated row, invokes user's callback
- Callback receives C struct (line_t*, order_t*, etc.)
- Callback responsibility: store/process the row

### 2. DBGen Converter (Phase 9 - New)
**File**: `include/tpch/dbgen_converter.hpp` (to create)

```cpp
// Conversion functions - one per table type
void append_lineitem_to_builders(const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_orders_to_builders(const void* row, ...);
void append_customer_to_builders(const void* row, ...);
// ... etc
```

**Responsibility**:
- Receive C struct (void* = actually line_t*, order_t*, etc.)
- Cast to correct type
- Extract fields
- Handle type conversions (int64 → float64, char[] → utf8, etc.)
- Append to Arrow builders

**Example** (lineitem):
```cpp
void append_lineitem_to_builders(const void* row, auto& builders) {
    auto* line = static_cast<const line_t*>(row);

    // Extract and convert each field
    auto* orderkey_builder = static_cast<arrow::Int64Builder*>(
        builders["l_orderkey"].get());
    orderkey_builder->Append(line->okey);

    auto* quantity_builder = static_cast<arrow::DoubleBuilder*>(
        builders["l_quantity"].get());
    // dbgen stores quantity as int, but schema expects float64
    quantity_builder->Append(static_cast<double>(line->quantity) / 100.0);

    // ... repeat for all 16 fields in lineitem ...
}
```

### 3. Main Driver Integration (Phase 9 - Modify)
**File**: `src/main.cpp`

**New Function**:
```cpp
void generate_with_dbgen(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    std::function<void(tpch::DBGenWrapper&,
                       std::function<void(const void*)>)> generate_fn)
{
    // 1. Create builders from schema
    // 2. Define callback to append rows to builders
    // 3. Call generate_fn with callback
    // 4. Flush batches when accumulated
    // 5. Return total row count
}
```

## Call Sequence: Generating Lineitem

```
1. main.cpp:main()
   └─ opts = parse_args()  // --use-dbgen, --scale-factor 1

2. main.cpp:main()
   └─ schema = DBGenWrapper::get_schema(TableType::LINEITEM)
      // Returns Arrow schema with 16 fields

3. main.cpp:main()
   └─ dbgen = DBGenWrapper(1)
      // Initialize with scale_factor=1

4. main.cpp:generate_with_dbgen()
   └─ builders = create_builders_from_schema(schema)
      // Create Int64Builder, DoubleBuilder, StringBuilder for each column

5. main.cpp:generate_with_dbgen()
   └─ append_callback = [&](const void* row) {
        append_lineitem_to_builders(row, builders);
        // Cast void* to line_t*, extract fields, append to builders
      }

6. main.cpp:generate_with_dbgen()
   └─ dbgen.generate_lineitem(append_callback, opts.max_rows)

7. DBGenWrapper::generate_lineitem()
   ├─ init_dbgen()  // Set scale=1 globally
   ├─ row_start(DBGEN_TABLE_LINE)  // dbgen internal marker
   ├─ for order_idx = 1 to 1,500,000:
   │   ├─ mk_order(order_idx, &order_struct, 0)  // dbgen C function
   │   ├─ for lineitem_idx = 0 to order_struct.lines (1-7):
   │   │   └─ append_callback(&order_struct.l[lineitem_idx])
   │   │      // This invokes the lambda from step 5
   │   │      // which casts to line_t* and appends fields
   │   └─ (repeat for next order)
   └─ row_stop(DBGEN_TABLE_LINE)

8. main.cpp:append_lineitem_to_builders()
   // Executed 6M times for lineitem at SF=1
   └─ Append row fields to builders

9. When batch size reached (10000 rows):
   └─ finish_batch() converts builders to RecordBatch
   └─ writer->write_batch(batch)  // Write to Parquet/CSV
   └─ reset_builders()  // Clear for next batch

10. After all rows exhausted:
    └─ Flush remaining < 10000 rows
    └─ writer->close()

11. Output:
    └─ /tmp/sample_data.parquet (6M rows, ~2-3 GB)
```

## C Struct Definitions (From dbgen)

All of these are defined in `src/dbgen/dbgen_wrapper.cpp` as extern "C" declarations:

### line_t (TPC-H lineitem)
```c
typedef struct {
    long long okey;              // Order key (1 to 1.5M per SF)
    long long partkey;           // Part key (1 to 200K per SF)
    long long suppkey;           // Supplier key (1 to 10K per SF)
    long long lcnt;              // Line number (1 to 7)
    long long quantity;          // Quantity (1-50)
    long long eprice;            // Extended price
    long long discount;          // Discount (0-10%, stored as decimal)
    long long tax;               // Tax (0-8%, stored as decimal)
    char rflag[1];               // Return flag: N, O, R
    char lstatus[1];             // Line status: O, F
    char cdate[11];              // Commit date (YYYY-MM-DD)
    char sdate[11];              // Ship date (YYYY-MM-DD)
    char rdate[11];              // Receipt date (YYYY-MM-DD)
    char shipinstruct[11];       // Ship instructions
    char shipmode[11];           // Ship mode
    char comment[45];            // Line comment (up to 44 chars)
    int clen;                    // Comment length
} line_t;
```

## Schema Alignment

### Arrow Schema (from DBGenWrapper::get_schema)
```cpp
return arrow::schema({
    field("l_orderkey", int64()),
    field("l_partkey", int64()),
    field("l_suppkey", int64()),
    field("l_linenumber", int64()),
    field("l_quantity", float64()),       // Converted from int
    field("l_extendedprice", float64()),
    field("l_discount", float64()),
    field("l_tax", float64()),
    field("l_returnflag", utf8()),        // Converted from char[1]
    field("l_linestatus", utf8()),
    field("l_commitdate", utf8()),        // Converted from char[11]
    field("l_shipdate", utf8()),
    field("l_receiptdate", utf8()),
    field("l_shipinstruct", utf8()),
    field("l_shipmode", utf8()),
    field("l_comment", utf8()),           // Converted from char[45]
});
```

### Type Conversions in Converter
| C Field | C Type | Arrow Field | Arrow Type | Conversion |
|---------|--------|-------------|-----------|------------|
| okey | long long | l_orderkey | int64 | Direct cast |
| quantity | long long | l_quantity | float64 | / 100.0 |
| eprice | long long | l_extendedprice | float64 | / 100.0 |
| rflag | char[1] | l_returnflag | utf8 | Extract char, to_string |
| cdate | char[11] | l_commitdate | utf8 | String from array |
| comment | char[45] | l_comment | utf8 | Truncate at clen |

## Error Handling Flow

```
dbgen C function (mk_order, etc.)
    │
    ├─ Success: returns >= 0
    │   └─ Proceed to next row
    │
    └─ Error: returns < 0
        └─ DBGenWrapper catches, throws exception
            └─ main.cpp catches, prints error, exits(1)
```

## Performance Expected

For lineitem at SF=1 (6M rows):

```
Generation rate:    1-2 million rows/second
Processing time:    3-6 seconds
Parquet output:     2-3 GB (uncompressed data)
Compressed (Snappy): 600-900 MB

Time breakdown:
- dbgen row generation:  2 seconds
- Arrow builder append:  1 second
- Parquet write:         2 seconds
- Total:                 ~5 seconds
```

For other tables:
```
orders:     1.5M rows @ SF=1   → ~5-10 seconds
customer:   150K rows @ SF=1   → <1 second
part:       200K rows @ SF=1   → <1 second
partsupp:   800K rows @ SF=1   → ~2-3 seconds
supplier:   10K rows @ SF=1    → <0.5 seconds
nation:     25 rows            → immediate
region:     5 rows             → immediate
```

## Integration Checklist

- [ ] Uncomment `src/dbgen/dbgen_wrapper.cpp` in CMakeLists.txt
- [ ] Create `include/tpch/dbgen_converter.hpp`
- [ ] Implement `src/dbgen/dbgen_converter.cpp` (8 functions, one per table)
- [ ] Modify `src/main.cpp`:
  - [ ] Add includes for dbgen_wrapper, dbgen_converter
  - [ ] Add `table` option to Options struct
  - [ ] Add CLI parsing for --table
  - [ ] Implement generate_with_dbgen() helper
  - [ ] Replace synthetic data loop with dbgen dispatch
- [ ] Update CMakeLists.txt to include dbgen_converter.cpp in TPCH_CORE_SOURCES
- [ ] Test compilation
- [ ] Run functional tests (SF=1 for each table)
- [ ] Validate output (row counts, schema, data correctness)
- [ ] Performance benchmark
- [ ] Update README.md with --use-dbgen instructions
- [ ] Commit as "Phase 9: Real DBGen Integration & Scale Factor Support"

