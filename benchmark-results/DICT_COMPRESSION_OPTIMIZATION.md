# Phase 3.3: Arrow Dictionary Encoding for Low-Cardinality TPC-H Columns

## Overview

Replace `utf8()` Arrow arrays with `dictionary(int8(), utf8())` for TPC-H columns with
known low cardinality (2–150 distinct values). Lance has native `DictionaryDataBlock`
support where `compute_stat()` is a complete **no-op** — bypassing HLL, XXH3, and all
statistics overhead entirely, not just via hints.

## Motivation

### Phase 3.2 Status (Pre-computed Cardinality Hints)
- Injected `lance.cardinality` field metadata → HLL skipped for 7 lineitem columns
- SF=5 lineitem baseline ~217K r/s → ~426K r/s (+96%)
- Limitation: still calls `compute_stat()`, reads metadata, allocates HLL state

### Phase 3.3 Goal (Dictionary Blocks)
- Pass `DictionaryArray<Int8Type, StringArray>` directly to Lance
- Lance routes to `DictionaryDataBlock` → `compute_stat()` is literal `{}` (empty body)
- No HLL, no XXH3, no cardinality tracking AT ALL for these columns
- Additional benefit: ~4–8× compression on index data (int8 vs full strings)

### Lance Code Evidence

`third_party/lance/rust/lance-encoding/src/statistics.rs` line 174:
```rust
Self::Dictionary(_) => {}   // ← complete no-op, zero overhead
```

`GetStat for DictionaryDataBlock` returns `None` for any stat.

`third_party/lance/rust/lance-encoding/src/data.rs` line 1590:
```rust
DataType::Dictionary(_, _) => arrow_dictionary_to_data_block(arrays, nulls.to_option())
```
Native support already present — no Lance modifications needed for core path.

---

## TPC-H Low-Cardinality Columns (from dists.dss)

### lineitem (4 columns → int8 indices)

| Column | Cardinality | Values |
|--------|------------|--------|
| `l_returnflag` | 3 | A, N, R |
| `l_linestatus` | 2 | F, O |
| `l_shipinstruct` | 4 | COLLECT COD, DELIVER IN PERSON, NONE, TAKE BACK RETURN |
| `l_shipmode` | 7 | AIR, FOB, MAIL, RAIL, REG AIR, SHIP, TRUCK |

### orders (2 columns → int8 indices)

| Column | Cardinality | Values |
|--------|------------|--------|
| `o_orderstatus` | 3 | F, O, P |
| `o_orderpriority` | 5 | 1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW |

### customer (1 column → int8 indices)

| Column | Cardinality | Values |
|--------|------------|--------|
| `c_mktsegment` | 5 | AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY |

### part (4 columns, p_type → uint8)

| Column | Cardinality | Values | Index type |
|--------|------------|--------|-----------|
| `p_mfgr` | 5 | Manufacturer#1 … Manufacturer#5 | int8 |
| `p_brand` | 25 | Brand#11 … Brand#55 | int8 |
| `p_container` | 40 | SM/MED/LG/JUMBO/WRAP × BOX/BAG/JAR/PKG/PACK/CAN/DRUM/CUP (5×8) | int8 |
| `p_type` | 150 | 3 syllables × 5 syllables × 10 syllables | uint8 |

Note: `p_type` with 150 values exceeds int8 range (max 127 signed). Use `uint8` (Arrow `UInt8Type`) or `int16`. Simplest: keep as utf8 with cardinality hint, or use `int16()` index.

---

## O(1) Encode Functions

All functions use switch on first character(s) — no hashing, no strcmp.

```cpp
inline int8_t encode_returnflag(char c) {
    switch (c) {
        case 'A': return 0;   // A
        case 'N': return 1;   // N
        case 'R': return 2;   // R
        default:  return 0;
    }
}

inline int8_t encode_linestatus(char c) {
    return c == 'F' ? 0 : 1;  // F=0, O=1
}

inline int8_t encode_shipinstruct(const char* s) {
    // C=COLLECT COD, D=DELIVER IN PERSON, N=NONE, T=TAKE BACK RETURN
    switch (s[0]) {
        case 'C': return 0;
        case 'D': return 1;
        case 'N': return 2;
        case 'T': return 3;
        default:  return 0;
    }
}

inline int8_t encode_shipmode(const char* s) {
    // AIR=0, FOB=1, MAIL=2, RAIL=3, REG AIR=4, SHIP=5, TRUCK=6
    switch (s[0]) {
        case 'A': return 0;   // AIR
        case 'F': return 1;   // FOB
        case 'M': return 2;   // MAIL
        case 'R': return s[1] == 'A' ? 3 : 4;  // RAIL vs REG AIR
        case 'S': return 5;   // SHIP
        case 'T': return 6;   // TRUCK
        default:  return 0;
    }
}

inline int8_t encode_orderstatus(char c) {
    switch (c) {
        case 'F': return 0;
        case 'O': return 1;
        case 'P': return 2;
        default:  return 0;
    }
}

inline int8_t encode_orderpriority(const char* s) {
    // "1-URGENT"=0, "2-HIGH"=1, "3-MEDIUM"=2, "4-NOT SPECIFIED"=3, "5-LOW"=4
    return (int8_t)(s[0] - '1');
}

inline int8_t encode_mktsegment(const char* s) {
    // AUTOMOBILE=0, BUILDING=1, FURNITURE=2, HOUSEHOLD=3, MACHINERY=4
    switch (s[0]) {
        case 'A': return 0;
        case 'B': return 1;
        case 'F': return 2;
        case 'H': return 3;
        case 'M': return 4;
        default:  return 0;
    }
}

inline int8_t encode_mfgr(const char* s) {
    // "Manufacturer#N" → index at position 13
    return (int8_t)(s[13] - '1');
}

inline int8_t encode_brand(const char* s) {
    // "Brand#XY" → X in [1-5], Y in [1-5]
    return (int8_t)((s[6] - '1') * 5 + (s[7] - '1'));
}

inline int8_t encode_container(const char* s) {
    // Prefix (5): SM=0, MED=1, LG=2, JUMBO=3, WRAP=4
    // Size   (8): BOX=0, BAG=1, JAR=2, PKG=3, PACK=4, CAN=5, DRUM=6, CUP=7
    int8_t prefix;
    switch (s[0]) {
        case 'S': prefix = s[1] == 'M' ? 0 : -1; break;  // SM
        case 'M': prefix = 1; break;  // MED
        case 'L': prefix = 2; break;  // LG
        case 'J': prefix = 3; break;  // JUMBO
        case 'W': prefix = 4; break;  // WRAP
        default:  prefix = 0;
    }
    // Find space separator, then parse size
    const char* sz = s;
    while (*sz && *sz != ' ') sz++;
    if (*sz) sz++;  // skip space
    int8_t size;
    switch (sz[0]) {
        case 'B': size = sz[1] == 'O' ? 0 : 1; break;  // BOX vs BAG
        case 'J': size = 2; break;   // JAR
        case 'P': size = sz[1] == 'K' ? 3 : 4; break;  // PKG vs PACK
        case 'C': size = sz[1] == 'A' ? 5 : 7; break;  // CAN vs CUP
        case 'D': size = 6; break;   // DRUM
        default:  size = 0;
    }
    return (int8_t)(prefix * 8 + size);
}
```

---

## Dictionary Definitions (Arrow side)

```cpp
// Dictionaries to pass into DictionaryArray constructor:

static auto RETURNFLAG_DICT = arrow::ArrayFromVector<arrow::StringType>({"A", "N", "R"});
static auto LINESTATUS_DICT = arrow::ArrayFromVector<arrow::StringType>({"F", "O"});
static auto SHIPINSTRUCT_DICT = arrow::ArrayFromVector<arrow::StringType>(
    {"COLLECT COD", "DELIVER IN PERSON", "NONE", "TAKE BACK RETURN"});
static auto SHIPMODE_DICT = arrow::ArrayFromVector<arrow::StringType>(
    {"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"});
static auto ORDERSTATUS_DICT = arrow::ArrayFromVector<arrow::StringType>({"F", "O", "P"});
static auto ORDERPRIORITY_DICT = arrow::ArrayFromVector<arrow::StringType>(
    {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"});
static auto MKTSEGMENT_DICT = arrow::ArrayFromVector<arrow::StringType>(
    {"AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"});
static auto MFGR_DICT = arrow::ArrayFromVector<arrow::StringType>(
    {"Manufacturer#1", "Manufacturer#2", "Manufacturer#3", "Manufacturer#4", "Manufacturer#5"});
```

Helper to build `DictionaryArray<Int8Type>` from an index buffer + static dictionary:
```cpp
std::shared_ptr<arrow::Array> build_dict_int8_array(
    const std::vector<int8_t>& indices,
    const std::shared_ptr<arrow::Array>& dictionary)
{
    auto index_arr = arrow::ArrayFromVector<arrow::Int8Type>(indices);
    auto dict_type = arrow::dictionary(arrow::int8(), arrow::utf8());
    return arrow::DictionaryArray::FromArrays(dict_type, index_arr, dictionary).ValueOrDie();
}
```

---

## Files to Modify

### 1. `src/dbgen/dbgen_wrapper.cpp` — Schema Definitions

Change `tpch_field(name, utf8(), cardinality)` to `tpch_field(name, dict_type)` for all
low-cardinality columns. Remove `lance.cardinality` metadata hints (redundant with dict blocks).

```cpp
// Before (Phase 3.2):
tpch_field("l_returnflag",   arrow::utf8(), 3)   // cardinality hint
// After (Phase 3.3):
tpch_field("l_returnflag",   arrow::dictionary(arrow::int8(), arrow::utf8()))
```

Affected columns:
- lineitem: `l_returnflag`, `l_linestatus`, `l_shipinstruct`, `l_shipmode`
- orders: `o_orderstatus`, `o_orderpriority`
- customer: `c_mktsegment`
- part: `p_mfgr`, `p_brand`, `p_container` (keep `p_type` as utf8 with hint — 150 values)

### 2. `include/tpch/zero_copy_converter.hpp` — New Declarations

Add declarations for int8 array builder and dict array builder:
```cpp
std::shared_ptr<arrow::Array> build_int8_array(const std::vector<int8_t>& values);

std::shared_ptr<arrow::Array> build_dict_int8_array(
    const std::vector<int8_t>& indices,
    const std::shared_ptr<arrow::Array>& dictionary);
```

### 3. `src/dbgen/zero_copy_converter.cpp` — Zero-Copy Conversion Path

Primary performance path. For each dict column:
1. Accumulate `int8_t` indices instead of `std::string_view`
2. Call `build_dict_int8_array(indices, DICT)` instead of `build_string_array(views)`

Pattern per batch:
```cpp
// Add index accumulation vectors alongside existing ones
std::vector<int8_t> returnflag_indices;
returnflag_indices.reserve(batch_size);

// In row loop:
returnflag_indices.push_back(encode_returnflag(l->returnflag[0]));

// In finalize:
arrays["l_returnflag"] = build_dict_int8_array(returnflag_indices, RETURNFLAG_DICT);
```

### 4. `src/dbgen/dbgen_converter.cpp` — Row-by-Row Conversion Path

Change `arrow::StringBuilder*` casts to `arrow::StringDictionaryBuilder<arrow::Int8Type>*`
for dict columns. The `Append(value)` API is identical.

```cpp
// Before:
auto* builder = dynamic_cast<arrow::StringBuilder*>(builders[col].get());
builder->Append(value);

// After:
using DictBuilder = arrow::StringDictionaryBuilder<arrow::Int8Type>;
auto* builder = dynamic_cast<DictBuilder*>(builders[col].get());
builder->Append(value);
```

### 5. `src/main.cpp` — Builder Creation

Add `DICTIONARY` case in `create_builders_from_schema`:
```cpp
} else if (field->type()->id() == arrow::Type::DICTIONARY) {
    auto builder = std::make_shared<arrow::StringDictionaryBuilder<arrow::Int8Type>>();
    builders[field->name()] = builder;
}
```

### 6. `third_party/lance-ffi/src/lib.rs` — Skip Compression Hints for Dict Fields

`apply_compression_metadata()` currently adds `lance-encoding:compression: lz4` to ALL
fields. Dictionary fields should be skipped (Lance manages dict encoding internally).

```rust
fn apply_compression_metadata(schema: &Schema) -> Schema {
    let fields: Vec<Field> = schema.fields().iter().map(|field| {
        let mut metadata = field.metadata().clone();
        // Skip compression hints for dictionary fields — Lance handles internally
        if !matches!(field.data_type(), DataType::Dictionary(_, _)) {
            metadata.insert(
                "lance-encoding:compression".to_string(),
                "lz4".to_string(),
            );
            match field.data_type() {
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    metadata.insert(
                        "lance-encoding:bss".to_string(),
                        "auto".to_string(),
                    );
                }
                _ => {}
            }
        }
        field.as_ref().clone().with_metadata(metadata)
    }).collect();
    Schema::new(fields).with_metadata(schema.metadata().clone())
}
```

---

## Implementation Order

1. **`third_party/lance-ffi/src/lib.rs`** — fix `apply_compression_metadata` (1 Rust file, build once)
2. **`src/dbgen/dbgen_wrapper.cpp`** — change schema field types (schema is the contract)
3. **`include/tpch/zero_copy_converter.hpp`** — add declarations
4. **`src/dbgen/zero_copy_converter.cpp`** — implement encode functions + dict array builders
5. **`src/dbgen/dbgen_converter.cpp`** — update row-by-row path
6. **`src/main.cpp`** — add DICTIONARY builder case

Build order: Rust FFI first → then C++:
```bash
# Step 1: Build Rust FFI
RUSTC=/snap/bin/rustc /snap/bin/cargo build --release \
    --manifest-path=third_party/lance-ffi/Cargo.toml

# Step 2: Incremental C++ build
cmake -DTPCH_ENABLE_LANCE=ON build/ && cmake --build build/ -j$(nproc)
```

---

## Expected Performance Impact

### Lineitem SF=5 (30M rows, 8 string columns)

| Phase | Strategy | Throughput |
|-------|----------|-----------|
| Baseline | No optimization | ~217K r/s |
| 3.2 (cardinality hints) | HLL skipped via metadata | ~426K r/s |
| 3.3 target (dict encoding) | compute_stat() = no-op | ~600K+ r/s |

### Data Size Impact (per SF=5 batch, 30M rows)

- `l_returnflag` (avg 1 byte): 30MB string → 30MB int8 (same size, but no encoding overhead)
- `l_shipmode` (avg 5 bytes): 150MB string → 30MB int8 + 50B dict (5× smaller)
- `l_shipinstruct` (avg 10 bytes): 300MB string → 30MB int8 + 200B dict (10× smaller)

Total string data eliminated for 4 lineitem columns: ~600MB per SF=5 run.

### Zero Stats Overhead

Phase 3.2: `compute_stat()` called, reads metadata, checks thread-local, skips HLL.
Phase 3.3: `compute_stat()` = `{}` — branch prediction, no memory access, no function call overhead.

---

## Notes and Caveats

1. **Schema type changes**: Lance files will store `dictionary(int8(), utf8())` instead of `utf8()`.
   For a benchmark tool this is acceptable. Read tools (DuckDB, Polars) can decode dict arrays.

2. **`p_type` with 150 values**: Exceeds `int8` signed range (max 127). Options:
   - Use `int16()` index type → `dictionary(int16(), utf8())`
   - Use `uint8()` → `dictionary(uint8(), utf8())`
   - Keep as `utf8()` with Phase 3.2 cardinality hint (150)
   Recommended: keep `p_type` as utf8 with cardinality hint for simplicity.

3. **`l_comment` and other high-cardinality strings**: Keep as utf8 with cardinality hint
   (Phase 3.2 already handles these — no dict encoding for high-cardinality columns).

4. **`--zero-copy` regression**: Zero-copy path (42K r/s) still unresolved. Dictionary
   encoding will be implemented in both zero-copy and non-zero-copy paths regardless.

5. **Benchmark cleanup**: Always `rm -rf /tmp/*.lance` before benchmarking — Lance appends
   fragments on each run rather than overwriting, causing fragment accumulation.
