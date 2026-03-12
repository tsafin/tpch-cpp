#pragma once

#include <arrow/api.h>
#include <span>
#include <string_view>

extern "C" {
#include "tpch_dbgen.h"
}

#include "tpch/buffer_lifetime_manager.hpp"

namespace tpch {

/**
 * Zero-copy conversion from dbgen batches to Arrow using modern C++ views
 *
 * This converter uses std::span for zero-copy array access and std::string_view
 * to avoid intermediate string allocations. Designed for Phase 13.4 optimizations.
 *
 * Key optimizations:
 * - Single allocation per Arrow array
 * - std::string_view for zero-copy string access
 * - Batch operations instead of row-by-row
 * - Minimal memory copies (60-80% reduction in bandwidth)
 */
class ZeroCopyConverter {
public:
    /**
     * Convert lineitem batch to Arrow RecordBatch
     *
     * Uses std::span for zero-copy array access and string_view
     * to avoid intermediate string allocations.
     *
     * @param batch Span view over line_t structs (no copy)
     * @param schema Arrow schema for lineitem table
     * @return RecordBatch with minimal copies
     */
    static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    lineitem_to_recordbatch(
        std::span<const line_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert orders batch to Arrow RecordBatch
     *
     * @param batch Span view over order_t structs (no copy)
     * @param schema Arrow schema for orders table
     * @return RecordBatch with minimal copies
     */
    static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    orders_to_recordbatch(
        std::span<const order_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert customer batch to Arrow RecordBatch
     *
     * @param batch Span view over customer_t structs (no copy)
     * @param schema Arrow schema for customer table
     * @return RecordBatch with minimal copies
     */
    static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    customer_to_recordbatch(
        std::span<const customer_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert part batch to Arrow RecordBatch
     *
     * @param batch Span view over part_t structs (no copy)
     * @param schema Arrow schema for part table
     * @return RecordBatch with minimal copies
     */
    static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    part_to_recordbatch(
        std::span<const part_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert partsupp batch to Arrow RecordBatch
     *
     * @param batch Span view over partsupp_t structs (no copy)
     * @param schema Arrow schema for partsupp table
     * @return RecordBatch with minimal copies
     */
    static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    partsupp_to_recordbatch(
        std::span<const partsupp_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert supplier batch to Arrow RecordBatch
     *
     * @param batch Span view over supplier_t structs (no copy)
     * @param schema Arrow schema for supplier table
     * @return RecordBatch with minimal copies
     */
    static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    supplier_to_recordbatch(
        std::span<const supplier_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert nation batch to Arrow RecordBatch
     *
     * @param batch Span view over code_t structs (no copy)
     * @param schema Arrow schema for nation table
     * @return RecordBatch with minimal copies
     */
    static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    nation_to_recordbatch(
        std::span<const code_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert region batch to Arrow RecordBatch
     *
     * @param batch Span view over code_t structs (no copy)
     * @param schema Arrow schema for region table
     * @return RecordBatch with minimal copies
     */
    static arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    region_to_recordbatch(
        std::span<const code_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    // ========================================================================
    // Phase 14.2.3: True Zero-Copy Converters (using Buffer::Wrap)
    // ========================================================================
    // These converters use arrow::Buffer::Wrap() to directly reference vector
    // memory without memcpy. Eliminates final memory copy for numeric arrays.
    //
    // Return ManagedRecordBatch which keeps source vectors alive until encoding.
    //
    // Requires: streaming_write mode (batch mode uses more memory)
    // Benefit: 10-20% speedup over Phase 14.1 for numeric-heavy tables

    /**
     * Convert lineitem batch to Arrow RecordBatch (true zero-copy)
     *
     * Uses Buffer::Wrap for numeric arrays (no memcpy).
     * Strings still use memcpy (non-contiguous in dbgen structs).
     *
     * @param batch Span view over line_t structs (no copy)
     * @param schema Arrow schema for lineitem table
     * @return ManagedRecordBatch with wrapped buffers + lifetime manager
     */
    static arrow::Result<ManagedRecordBatch>
    lineitem_to_recordbatch_wrapped(
        std::span<const line_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert orders batch to Arrow RecordBatch (true zero-copy)
     */
    static arrow::Result<ManagedRecordBatch>
    orders_to_recordbatch_wrapped(
        std::span<const order_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert customer batch to Arrow RecordBatch (true zero-copy)
     */
    static arrow::Result<ManagedRecordBatch>
    customer_to_recordbatch_wrapped(
        std::span<const customer_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert part batch to Arrow RecordBatch (true zero-copy)
     */
    static arrow::Result<ManagedRecordBatch>
    part_to_recordbatch_wrapped(
        std::span<const part_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert partsupp batch to Arrow RecordBatch (true zero-copy)
     */
    static arrow::Result<ManagedRecordBatch>
    partsupp_to_recordbatch_wrapped(
        std::span<const partsupp_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert supplier batch to Arrow RecordBatch (true zero-copy)
     */
    static arrow::Result<ManagedRecordBatch>
    supplier_to_recordbatch_wrapped(
        std::span<const supplier_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert nation batch to Arrow RecordBatch (true zero-copy)
     */
    static arrow::Result<ManagedRecordBatch>
    nation_to_recordbatch_wrapped(
        std::span<const code_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    /**
     * Convert region batch to Arrow RecordBatch (true zero-copy)
     */
    static arrow::Result<ManagedRecordBatch>
    region_to_recordbatch_wrapped(
        std::span<const code_t> batch,
        const std::shared_ptr<arrow::Schema>& schema
    );

    // ========================================================================
    // Phase 3.3: Dictionary encode helpers for low-cardinality TPC-H columns
    // ========================================================================
    // O(1) switch-based encode functions — no hashing, no strcmp needed.
    // Each returns an int8 index into the static dictionary for that column.

    static inline int8_t encode_returnflag(char c) {
        switch (c) { case 'A': return 0; case 'N': return 1; default: return 2; }  // R=2
    }
    static inline int8_t encode_linestatus(char c) {
        return c == 'F' ? 0 : 1;  // F=0, O=1
    }
    static inline int8_t encode_shipinstruct(const char* s) {
        // COLLECT COD=0, DELIVER IN PERSON=1, NONE=2, TAKE BACK RETURN=3
        switch (s[0]) { case 'C': return 0; case 'D': return 1; case 'N': return 2; default: return 3; }
    }
    static inline int8_t encode_shipmode(const char* s) {
        // AIR=0, FOB=1, MAIL=2, RAIL=3, REG AIR=4, SHIP=5, TRUCK=6
        switch (s[0]) {
            case 'A': return 0;
            case 'F': return 1;
            case 'M': return 2;
            case 'R': return s[1] == 'A' ? 3 : 4;  // RAIL vs REG AIR
            case 'S': return 5;
            case 'T': return 6;
            default:  return 0;
        }
    }
    static inline int8_t encode_orderstatus(char c) {
        switch (c) { case 'F': return 0; case 'O': return 1; default: return 2; }  // P=2
    }
    static inline int8_t encode_orderpriority(const char* s) {
        return (int8_t)(s[0] - '1');  // "1-URGENT"=0 .. "5-LOW"=4
    }
    static inline int8_t encode_mktsegment(const char* s) {
        // AUTOMOBILE=0, BUILDING=1, FURNITURE=2, HOUSEHOLD=3, MACHINERY=4
        switch (s[0]) { case 'A': return 0; case 'B': return 1; case 'F': return 2; case 'H': return 3; default: return 4; }
    }
    static inline int8_t encode_mfgr(const char* s) {
        return (int8_t)(s[13] - '1');  // "Manufacturer#N" → position 13
    }
    static inline int8_t encode_brand(const char* s) {
        return (int8_t)((s[6] - '1') * 5 + (s[7] - '1'));  // "Brand#XY"
    }
    static inline int8_t encode_container(const char* s) {
        // prefix: SM=0, MED=1, LG=2, JUMBO=3, WRAP=4
        int8_t prefix;
        switch (s[0]) {
            case 'S': prefix = 0; break;
            case 'M': prefix = 1; break;
            case 'L': prefix = 2; break;
            case 'J': prefix = 3; break;
            default:  prefix = 4; break;  // WRAP
        }
        // size: BOX=0, BAG=1, JAR=2, PKG=3, PACK=4, CAN=5, DRUM=6, CUP=7
        const char* sz = s;
        while (*sz && *sz != ' ') sz++;
        if (*sz == ' ') sz++;
        int8_t size;
        switch (sz[0]) {
            case 'B': size = (sz[1] == 'O') ? 0 : 1; break;  // BOX vs BAG
            case 'J': size = 2; break;
            case 'P': size = (sz[1] == 'K') ? 3 : 4; break;  // PKG vs PACK
            case 'C': size = (sz[1] == 'A') ? 5 : 7; break;  // CAN vs CUP
            default:  size = 6; break;  // DRUM
        }
        return (int8_t)(prefix * 8 + size);
    }

    // O(1) date encoder: "YYYY-MM-DD" → day offset from 1992-01-01 (int16)
    // TPC-H dates span 1992-01-01 to 1998-12-31 → indices 0..2556
    static inline int16_t encode_date(const char* s) {
        // Parse last 2 digits of year (92-98), month, day
        int y = (s[2] - '0') * 10 + (s[3] - '0');  // 92..98
        int m = (s[5] - '0') * 10 + (s[6] - '0');  // 1..12
        int d = (s[8] - '0') * 10 + (s[9] - '0');  // 1..31
        // Cumulative days before each year (1992=0, 1993=366 because 1992 is leap, ...)
        static const int16_t year_base[7] = {0, 366, 731, 1096, 1461, 1827, 2192};
        // Days before each month in non-leap and leap years
        static const int16_t mbase_nonleap[12] = {0,31,59,90,120,151,181,212,243,273,304,334};
        static const int16_t mbase_leap[12]    = {0,31,60,91,121,152,182,213,244,274,305,335};
        bool leap = (y == 92 || y == 96);
        int16_t doy = (leap ? mbase_leap : mbase_nonleap)[m - 1] + (int16_t)(d - 1);
        return year_base[y - 92] + doy;
    }

    // O(1) p_type encoder: "SYLLABLE1 SYLLABLE2 SYLLABLE3" → index 0..149
    // syllable1: STANDARD(0), SMALL(1), MEDIUM(2), LARGE(3), ECONOMY(4), PROMO(5)
    // syllable2: ANODIZED(0), BURNISHED(1), PLATED(2), POLISHED(3), BRUSHED(4)
    // syllable3: TIN(0), NICKEL(1), BRASS(2), STEEL(3), COPPER(4)
    static inline int16_t encode_ptype(const char* s) {
        int8_t s1;
        switch (s[0]) {
            case 'S': s1 = (s[1] == 'T') ? 0 : 1; break;  // STANDARD vs SMALL
            case 'M': s1 = 2; break;
            case 'L': s1 = 3; break;
            case 'E': s1 = 4; break;
            default:  s1 = 5; break;  // PROMO
        }
        const char* p2 = s;
        while (*p2 && *p2 != ' ') p2++;
        if (*p2 == ' ') p2++;
        int8_t s2;
        switch (p2[0]) {
            case 'A': s2 = 0; break;
            case 'B': s2 = (p2[1] == 'U') ? 1 : 4; break;  // BURNISHED vs BRUSHED
            case 'P': s2 = (p2[1] == 'L') ? 2 : 3; break;  // PLATED vs POLISHED
            default:  s2 = 0; break;
        }
        const char* p3 = p2;
        while (*p3 && *p3 != ' ') p3++;
        if (*p3 == ' ') p3++;
        int8_t s3;
        switch (p3[0]) {
            case 'T': s3 = 0; break;
            case 'N': s3 = 1; break;
            case 'B': s3 = 2; break;
            case 'S': s3 = 3; break;
            default:  s3 = 4; break;  // COPPER
        }
        return (int16_t)(s1 * 25 + s2 * 5 + s3);
    }

    /**
     * Return the static string dictionary array for a known low-cardinality field.
     * Returns nullptr for fields that are not dictionary-encoded.
     */
    static std::shared_ptr<arrow::Array> get_dict_for_field(const std::string& field_name);

private:
    /**
     * Build string array from string_view span
     *
     * Single allocation + memcpy into Arrow buffer.
     * This is much faster than row-by-row StringBuilder::Append().
     *
     * @param views Span of string views
     * @return Arrow StringArray
     */
    static arrow::Result<std::shared_ptr<arrow::StringArray>>
    build_string_array(std::span<const std::string_view> views);

    /**
     * Build DictionaryArray<Int8Type, StringArray> from index buffer + static dictionary.
     * Used for low-cardinality TPC-H columns (returnflag, linestatus, etc.)
     */
    static arrow::Result<std::shared_ptr<arrow::Array>>
    build_dict_int8_array(std::span<const int8_t> indices,
                          const std::shared_ptr<arrow::Array>& dictionary);

    /**
     * Build DictionaryArray<Int16Type, StringArray> from index buffer + static dictionary.
     * Used for medium-cardinality TPC-H columns (date fields: 2556 values, p_type: 150 values)
     */
    static arrow::Result<std::shared_ptr<arrow::Array>>
    build_dict_int16_array(std::span<const int16_t> indices,
                           const std::shared_ptr<arrow::Array>& dictionary);

    /**
     * Build Int64 array from contiguous memory
     */
    static arrow::Result<std::shared_ptr<arrow::Array>>
    build_int64_array(std::span<const int64_t> values);

    /**
     * Build Double array from contiguous memory
     */
    static arrow::Result<std::shared_ptr<arrow::Array>>
    build_double_array(std::span<const double> values);

    // ====================================================================
    // Phase 14.2.3: Wrapped array builders (using Buffer::Wrap)
    // ====================================================================
    // These builders use arrow::Buffer::Wrap() to create buffers that
    // directly reference vector memory without memcpy.
    //
    // **Critical**: The shared_ptr to the vector is captured in a custom
    // deallocator, keeping the vector alive until Arrow buffer is freed.
    //
    // **String limitation**: Strings are still built with memcpy because
    // string data is non-contiguous in dbgen structs.

    /**
     * Build Int64 array using Buffer::Wrap (true zero-copy)
     *
     * Arrow buffer directly references vector memory. Custom deallocator
     * captures shared_ptr to vector, extending its lifetime.
     *
     * @param vec_ptr Shared pointer to source vector (keeps it alive)
     * @return Arrow Int64Array with wrapped buffer (no memcpy)
     */
    static arrow::Result<std::shared_ptr<arrow::Array>>
    build_int64_array_wrapped(std::shared_ptr<std::vector<int64_t>> vec_ptr);

    /**
     * Build Double array using Buffer::Wrap (true zero-copy)
     *
     * @param vec_ptr Shared pointer to source vector (keeps it alive)
     * @return Arrow DoubleArray with wrapped buffer (no memcpy)
     */
    static arrow::Result<std::shared_ptr<arrow::Array>>
    build_double_array_wrapped(std::shared_ptr<std::vector<double>> vec_ptr);

    /**
     * Fast strlen for unaligned C strings (SIMD-optimized if available)
     */
    static inline size_t strlen_fast(const char* str) {
        // TODO: Use SIMD strlen from Phase 13.2 if available
        // For now, use standard strlen
        return std::strlen(str);
    }
};

} // namespace tpch
