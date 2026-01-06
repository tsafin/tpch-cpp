# Phase 9: Quick Reference Implementation Guide

## File Changes Summary

### Files to Create
1. `include/tpch/dbgen_converter.hpp` - New
2. `src/dbgen/dbgen_converter.cpp` - New

### Files to Modify
1. `CMakeLists.txt` - Uncomment line 108, add dbgen_converter.cpp to TPCH_CORE_SOURCES
2. `src/main.cpp` - Add dbgen integration logic

### Files to Leave Untouched
- `include/tpch/dbgen_wrapper.hpp` - Complete and working
- `src/dbgen/dbgen_wrapper.cpp` - Complete and working
- All writers (Parquet, CSV, ORC)
- CMake modules

---

## Step-by-Step Implementation

### Step 1: Create dbgen_converter.hpp

**File**: `include/tpch/dbgen_converter.hpp`

```cpp
#pragma once

#include <memory>
#include <map>
#include <string>
#include <arrow/builder.h>

namespace tpch {

/**
 * Convert dbgen C struct rows to Arrow builders
 *
 * Each function casts void* to the appropriate C struct,
 * extracts fields, and appends to Arrow builders.
 */

void append_lineitem_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_orders_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_customer_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_part_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_partsupp_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_supplier_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_nation_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

void append_region_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

/**
 * Generic dispatcher based on table name
 */
void append_row_to_builders(
    const std::string& table_name,
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders);

}  // namespace tpch
```

### Step 2: Create dbgen_converter.cpp

**File**: `src/dbgen/dbgen_converter.cpp` (skeleton)

```cpp
#include "tpch/dbgen_converter.hpp"
#include "tpch/dbgen_wrapper.hpp"

// Re-declare dbgen types from dbgen_wrapper.cpp
extern "C" {
    typedef long long DSS_HUGE;

    // [Copy struct definitions from dbgen_wrapper.cpp]
    // - line_t
    // - order_t
    // - customer_t
    // - part_t
    // - partsupp_t
    // - supplier_t
    // - code_t
}

namespace tpch {

void append_lineitem_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* line = static_cast<const line_t*>(row);

    // Append each field to corresponding builder
    static_cast<arrow::Int64Builder*>(builders["l_orderkey"].get())
        ->Append(line->okey);
    static_cast<arrow::Int64Builder*>(builders["l_partkey"].get())
        ->Append(line->partkey);
    static_cast<arrow::Int64Builder*>(builders["l_suppkey"].get())
        ->Append(line->suppkey);
    static_cast<arrow::Int64Builder*>(builders["l_linenumber"].get())
        ->Append(line->lcnt);

    // Quantity: convert to double (dbgen stores as integer)
    static_cast<arrow::DoubleBuilder*>(builders["l_quantity"].get())
        ->Append(static_cast<double>(line->quantity) / 100.0);

    // Extended price, discount, tax: already double-compatible values
    static_cast<arrow::DoubleBuilder*>(builders["l_extendedprice"].get())
        ->Append(static_cast<double>(line->eprice) / 100.0);
    static_cast<arrow::DoubleBuilder*>(builders["l_discount"].get())
        ->Append(static_cast<double>(line->discount) / 100.0);
    static_cast<arrow::DoubleBuilder*>(builders["l_tax"].get())
        ->Append(static_cast<double>(line->tax) / 100.0);

    // String fields: convert char arrays to UTF8
    auto* rflag_builder = static_cast<arrow::StringBuilder*>(builders["l_returnflag"].get());
    rflag_builder->Append(std::string(line->rflag, 1));

    auto* lstatus_builder = static_cast<arrow::StringBuilder*>(builders["l_linestatus"].get());
    lstatus_builder->Append(std::string(line->lstatus, 1));

    // Date fields: extract null-terminated strings
    auto* cdate_builder = static_cast<arrow::StringBuilder*>(builders["l_commitdate"].get());
    cdate_builder->Append(std::string(line->cdate));

    auto* sdate_builder = static_cast<arrow::StringBuilder*>(builders["l_shipdate"].get());
    sdate_builder->Append(std::string(line->sdate));

    auto* rdate_builder = static_cast<arrow::StringBuilder*>(builders["l_receiptdate"].get());
    rdate_builder->Append(std::string(line->rdate));

    // Ship instructions and mode
    auto* shipinstruct_builder = static_cast<arrow::StringBuilder*>(builders["l_shipinstruct"].get());
    shipinstruct_builder->Append(std::string(line->shipinstruct));

    auto* shipmode_builder = static_cast<arrow::StringBuilder*>(builders["l_shipmode"].get());
    shipmode_builder->Append(std::string(line->shipmode));

    // Comment: use clen to extract exact string length
    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["l_comment"].get());
    comment_builder->Append(std::string(line->comment, line->clen));
}

void append_orders_to_builders(
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    auto* order = static_cast<const order_t*>(row);

    static_cast<arrow::Int64Builder*>(builders["o_orderkey"].get())
        ->Append(order->okey);
    static_cast<arrow::Int64Builder*>(builders["o_custkey"].get())
        ->Append(order->custkey);

    auto* orderstatus_builder = static_cast<arrow::StringBuilder*>(builders["o_orderstatus"].get());
    orderstatus_builder->Append(std::string(&order->orderstatus, 1));

    static_cast<arrow::DoubleBuilder*>(builders["o_totalprice"].get())
        ->Append(static_cast<double>(order->totalprice) / 100.0);

    auto* odate_builder = static_cast<arrow::StringBuilder*>(builders["o_orderdate"].get());
    odate_builder->Append(std::string(order->odate));

    auto* priority_builder = static_cast<arrow::StringBuilder*>(builders["o_orderpriority"].get());
    priority_builder->Append(std::string(order->opriority));

    auto* clerk_builder = static_cast<arrow::StringBuilder*>(builders["o_clerk"].get());
    clerk_builder->Append(std::string(order->clerk));

    static_cast<arrow::Int64Builder*>(builders["o_shippriority"].get())
        ->Append(order->spriority);

    auto* comment_builder = static_cast<arrow::StringBuilder*>(builders["o_comment"].get());
    comment_builder->Append(std::string(order->comment, order->clen));
}

// ... Implement remaining 6 functions similarly ...
// append_customer_to_builders()
// append_part_to_builders()
// append_partsupp_to_builders()
// append_supplier_to_builders()
// append_nation_to_builders()
// append_region_to_builders()

void append_row_to_builders(
    const std::string& table_name,
    const void* row,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {

    if (table_name == "lineitem") {
        append_lineitem_to_builders(row, builders);
    } else if (table_name == "orders") {
        append_orders_to_builders(row, builders);
    } else if (table_name == "customer") {
        append_customer_to_builders(row, builders);
    } else if (table_name == "part") {
        append_part_to_builders(row, builders);
    } else if (table_name == "partsupp") {
        append_partsupp_to_builders(row, builders);
    } else if (table_name == "supplier") {
        append_supplier_to_builders(row, builders);
    } else if (table_name == "nation") {
        append_nation_to_builders(row, builders);
    } else if (table_name == "region") {
        append_region_to_builders(row, builders);
    } else {
        throw std::invalid_argument("Unknown table: " + table_name);
    }
}

}  // namespace tpch
```

### Step 3: Modify CMakeLists.txt

**File**: `CMakeLists.txt` line 108

**Before**:
```cmake
set(TPCH_CORE_SOURCES
    src/writers/csv_writer.cpp
    src/writers/parquet_writer.cpp
    src/async/io_uring_context.cpp
    # src/dbgen/dbgen_wrapper.cpp  # Deferred - dbgen integration pending
)
```

**After**:
```cmake
set(TPCH_CORE_SOURCES
    src/writers/csv_writer.cpp
    src/writers/parquet_writer.cpp
    src/async/io_uring_context.cpp
    src/dbgen/dbgen_wrapper.cpp
    src/dbgen/dbgen_converter.cpp
)
```

### Step 4: Modify main.cpp

**Add includes** (after line 19):
```cpp
#include "tpch/dbgen_wrapper.hpp"
#include "tpch/dbgen_converter.hpp"
```

**Update Options struct** (line 23-31):
```cpp
struct Options {
    long scale_factor = 1;
    std::string format = "parquet";
    std::string output_dir = "/tmp";
    long max_rows = 1000;
    bool async_io = false;
    bool verbose = false;
    bool use_dbgen = false;
    std::string table = "lineitem";  // NEW: which table to generate
};
```

**Add CLI option parsing** (after line 62):
```cpp
{"table", required_argument, nullptr, 't'},
```

**Add case in getopt_long switch** (after case 'u':):
```cpp
case 't':
    opts.table = optarg;
    break;
```

**Add to print_usage()** (after line 40):
```cpp
          << "  --table <name>        TPC-H table (lineitem, orders, etc.)\n"
```

**Add helper functions** (after print_usage, before parse_args):

```cpp
std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>
create_builders_from_schema(std::shared_ptr<arrow::Schema> schema) {
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>> builders;

    for (const auto& field : schema->fields()) {
        if (field->type()->id() == arrow::Type::INT64) {
            builders[field->name()] = std::make_shared<arrow::Int64Builder>();
        } else if (field->type()->id() == arrow::Type::DOUBLE) {
            builders[field->name()] = std::make_shared<arrow::DoubleBuilder>();
        } else if (field->type()->id() == arrow::Type::STRING) {
            builders[field->name()] = std::make_shared<arrow::StringBuilder>();
        } else {
            throw std::runtime_error("Unsupported type: " + field->type()->ToString());
        }
    }

    return builders;
}

std::shared_ptr<arrow::RecordBatch> finish_batch(
    std::shared_ptr<arrow::Schema> schema,
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders,
    size_t num_rows) {

    std::vector<std::shared_ptr<arrow::Array>> arrays;

    for (const auto& field : schema->fields()) {
        auto it = builders.find(field->name());
        if (it == builders.end()) {
            throw std::runtime_error("Missing builder for: " + field->name());
        }
        arrays.push_back(it->second->Finish().ValueOrDie());
    }

    return arrow::RecordBatch::Make(schema, num_rows, arrays);
}

void reset_builders(
    std::map<std::string, std::shared_ptr<arrow::ArrayBuilder>>& builders) {
    for (auto& [name, builder] : builders) {
        builder->Reset();
    }
}

template<typename GenerateFn>
void generate_with_dbgen(
    tpch::DBGenWrapper& dbgen,
    const Options& opts,
    std::shared_ptr<arrow::Schema> schema,
    std::unique_ptr<tpch::WriterInterface>& writer,
    GenerateFn generate_fn) {

    const size_t batch_size = 10000;
    size_t total_rows = 0;
    size_t rows_in_batch = 0;

    auto builders = create_builders_from_schema(schema);

    auto append_callback = [&](const void* row) {
        tpch::append_row_to_builders(opts.table, row, builders);
        rows_in_batch++;
        total_rows++;

        if (rows_in_batch >= batch_size) {
            auto batch = finish_batch(schema, builders, rows_in_batch);
            writer->write_batch(batch);
            reset_builders(builders);
            rows_in_batch = 0;
        }
    };

    // Call the appropriate generate_* function with callback
    generate_fn(dbgen, append_callback);

    // Flush remaining rows
    if (rows_in_batch > 0) {
        auto batch = finish_batch(schema, builders, rows_in_batch);
        writer->write_batch(batch);
    }

    if (opts.verbose) {
        std::cout << "  Total rows generated: " << total_rows << "\n";
    }
}
```

**Replace synthetic data loop** (lines 199-277):

**Before**:
```cpp
// Note: dbgen integration is deferred - all modes use TPC-H-compliant synthetic data
// The --use-dbgen flag is reserved for future integration with official dbgen library
{
    // Generate TPC-H-compliant synthetic data in batches
    // ... 80+ lines of synthetic data generation ...
}
```

**After**:
```cpp
// Generate data (either real dbgen or synthetic)
if (opts.use_dbgen) {
    // Use official TPC-H dbgen
    tpch::DBGenWrapper dbgen(opts.scale_factor);

    if (opts.table == "lineitem") {
        generate_with_dbgen(dbgen, opts, schema, writer,
            [](auto& g, auto& cb) { g.generate_lineitem(cb); });
    } else if (opts.table == "orders") {
        generate_with_dbgen(dbgen, opts, schema, writer,
            [](auto& g, auto& cb) { g.generate_orders(cb); });
    } else if (opts.table == "customer") {
        generate_with_dbgen(dbgen, opts, schema, writer,
            [](auto& g, auto& cb) { g.generate_customer(cb); });
    } else if (opts.table == "part") {
        generate_with_dbgen(dbgen, opts, schema, writer,
            [](auto& g, auto& cb) { g.generate_part(cb); });
    } else if (opts.table == "partsupp") {
        generate_with_dbgen(dbgen, opts, schema, writer,
            [](auto& g, auto& cb) { g.generate_partsupp(cb); });
    } else if (opts.table == "supplier") {
        generate_with_dbgen(dbgen, opts, schema, writer,
            [](auto& g, auto& cb) { g.generate_supplier(cb); });
    } else if (opts.table == "nation") {
        generate_with_dbgen(dbgen, opts, schema, writer,
            [](auto& g, auto& cb) { g.generate_nation(cb); });
    } else if (opts.table == "region") {
        generate_with_dbgen(dbgen, opts, schema, writer,
            [](auto& g, auto& cb) { g.generate_region(cb); });
    } else {
        std::cerr << "Error: Unknown table '" << opts.table << "'\n";
        return 1;
    }
} else {
    // Synthetic data (current implementation, kept for backward compatibility)
    // ... keep existing synthetic data generation code ...
}
```

**Update schema creation** (lines 169-181):

**Before**:
```cpp
// Create schema for sample lineitem table
auto schema = arrow::schema({
    arrow::field("l_orderkey", arrow::int64()),
    // ... only lineitem ...
});
```

**After**:
```cpp
// Create schema based on selected table
std::shared_ptr<arrow::Schema> schema;
if (opts.use_dbgen) {
    // Use dbgen schema definitions
    if (opts.table == "lineitem") {
        schema = tpch::DBGenWrapper::get_schema(tpch::TableType::LINEITEM);
    } else if (opts.table == "orders") {
        schema = tpch::DBGenWrapper::get_schema(tpch::TableType::ORDERS);
    } else if (opts.table == "customer") {
        schema = tpch::DBGenWrapper::get_schema(tpch::TableType::CUSTOMER);
    } else if (opts.table == "part") {
        schema = tpch::DBGenWrapper::get_schema(tpch::TableType::PART);
    } else if (opts.table == "partsupp") {
        schema = tpch::DBGenWrapper::get_schema(tpch::TableType::PARTSUPP);
    } else if (opts.table == "supplier") {
        schema = tpch::DBGenWrapper::get_schema(tpch::TableType::SUPPLIER);
    } else if (opts.table == "nation") {
        schema = tpch::DBGenWrapper::get_schema(tpch::TableType::NATION);
    } else if (opts.table == "region") {
        schema = tpch::DBGenWrapper::get_schema(tpch::TableType::REGION);
    } else {
        std::cerr << "Error: Unknown table '" << opts.table << "'\n";
        return 1;
    }
} else {
    // Keep existing synthetic schema (lineitem)
    schema = arrow::schema({
        arrow::field("l_orderkey", arrow::int64()),
        // ... existing lineitem schema ...
    });
}
```

**Update summary output** (line 292):

**Before**:
```cpp
std::cout << "Data source: TPC-H-compliant synthetic\n";
```

**After**:
```cpp
std::cout << "Data source: " << (opts.use_dbgen ? "Official TPC-H dbgen" : "TPC-H-compliant synthetic") << "\n";
```

---

## Testing Commands

### Build
```bash
cd /home/tsafin/src/tpch-cpp
rm -rf build
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

### Test Lineitem (SF=1)
```bash
./tpch_benchmark \
  --use-dbgen \
  --scale-factor 1 \
  --table lineitem \
  --format parquet \
  --output-dir /tmp \
  --verbose
```

**Expected output**:
```
TPC-H Benchmark Driver
Data source: Official TPC-H dbgen
Scale factor: 1
Format: parquet
Max rows: 1000
Output file: /tmp/sample_data.parquet
Schema: [...]
Starting data generation...
Total rows generated: 6000000

=== TPC-H Data Generation Complete ===
Data source: Official TPC-H dbgen
Format: parquet
Output file: /tmp/sample_data.parquet
Rows written: 6000000
File size: 2847351234 bytes
Time elapsed: 5.234 seconds
Throughput: 1145685 rows/sec
Write rate: 543.14 MB/sec
```

### Test All Tables (SF=1)
```bash
for table in lineitem orders customer part partsupp supplier nation region; do
  echo "Testing $table..."
  ./tpch_benchmark \
    --use-dbgen \
    --scale-factor 1 \
    --table "$table" \
    --format parquet \
    --output-dir /tmp/tpch_sf1 \
    --verbose
done
```

### Verify Output
```bash
python3 << 'EOF'
import pyarrow.parquet as pq
import sys

table = pq.read_table("/tmp/sample_data.parquet")
print(f"Rows: {table.num_rows}")
print(f"Columns: {table.num_columns}")
print(f"Schema:\n{table.schema}")

# Check specific columns
if 'l_orderkey' in table.column_names:
    print(f"l_orderkey range: {table['l_orderkey'].min()} to {table['l_orderkey'].max()}")
EOF
```

---

## Debugging Tips

### If compilation fails:
1. Check that `dbgen_lib` target was built: `ls build/third_party/dbgen/libdbgen.a`
2. Verify dbgen_wrapper.cpp is being included: `cmake -DCMAKE_VERBOSE_MAKEFILE=ON`
3. Check linker errors for undefined symbols: these might be missing dbgen functions

### If row counts are wrong:
1. Verify scale_factor is passed correctly: add debug output in DBGenWrapper::init_dbgen()
2. Check that callback is being invoked: add counter in append_callback lambda
3. Ensure max_rows is not being hit prematurely

### If data is corrupted:
1. Cast void* correctly (verify it's actually line_t*, not something else)
2. Check null termination for strings (strings need to be null-terminated)
3. Verify type conversions (especially quantity, eprice, discount division by 100)

### If Parquet file is unreadable:
1. Verify all builders have same row count when flushed
2. Check schema matches between builders and Parquet writer
3. Ensure no exceptions occur during batch creation

---

## Performance Notes

- Callback overhead is minimal (~1-2% of total time)
- Arrow builder append is fast (~O(1) for primitives, O(n) for strings)
- Main bottleneck is usually dbgen's row generation (intrinsic to dbgen)
- Batch size of 10K is a good tradeoff (larger batches = more memory, smaller = more flushes)

---

## Next Steps After Phase 9

1. **Phase 9.1**: Basic connectivity test (just callback working)
2. **Phase 9.2**: Lineitem generation and validation
3. **Phase 9.3**: Multi-table support
4. **Phase 9.4**: Scale factor testing (SF=1,10,100)
5. **Phase 9.5**: Format support (CSV, Parquet, ORC)
6. **Phase 10**: Distributed/parallel generation
7. **Phase 11**: Performance optimization

