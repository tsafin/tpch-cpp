#ifndef TPCH_AVRO_WRITER_HPP
#define TPCH_AVRO_WRITER_HPP

#include <vector>
#include <string>
#include <fstream>
#include <array>
#include <random>
#include <cstring>
#include <stdexcept>

namespace tpch {

/**
 * Avro binary encoding primitives and container file writer.
 * Hand-rolled implementation with no external dependencies beyond STL.
 * Encodes and writes Avro container files (.avro) with Null codec.
 */

namespace avro_detail {

/**
 * Write zigzag-encoded varint to buffer.
 * Zigzag: (n << 1) ^ (n >> 63) for 64-bit, then unsigned varint.
 */
inline void write_zigzag_long(std::vector<uint8_t>& buf, int64_t n) {
    uint64_t zigzag = (static_cast<uint64_t>(n) << 1) ^ (n >> 63);
    while ((zigzag & 0xFFFFFFFFFFFFFF80ULL) != 0) {
        buf.push_back(static_cast<uint8_t>((zigzag & 0x7F) | 0x80));
        zigzag >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(zigzag & 0x7F));
}

/**
 * Write zigzag-encoded varint to buffer (32-bit).
 */
inline void write_zigzag_int(std::vector<uint8_t>& buf, int32_t n) {
    uint32_t zigzag = (static_cast<uint32_t>(n) << 1) ^ (n >> 31);
    while ((zigzag & 0xFFFFFF80U) != 0) {
        buf.push_back(static_cast<uint8_t>((zigzag & 0x7F) | 0x80));
        zigzag >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(zigzag & 0x7F));
}

/**
 * Write Avro string: varint length + UTF-8 bytes.
 */
inline void write_avro_string(std::vector<uint8_t>& buf, const std::string& s) {
    write_zigzag_long(buf, static_cast<int64_t>(s.size()));
    buf.insert(buf.end(), s.begin(), s.end());
}

/**
 * Write Avro bytes: varint length + raw bytes.
 */
inline void write_avro_bytes(std::vector<uint8_t>& buf, const void* data, size_t len) {
    write_zigzag_long(buf, static_cast<int64_t>(len));
    const uint8_t* ptr = static_cast<const uint8_t*>(data);
    buf.insert(buf.end(), ptr, ptr + len);
}

/**
 * Write union index 0 (null). Always 0x00.
 */
inline void write_union_null(std::vector<uint8_t>& buf) {
    buf.push_back(0x00);
}

/**
 * Write union type index (non-null). Zigzag-encoded varint of the index.
 * Index 0 = null (use write_union_null instead).
 * Index 1 = type at position 1 in union.
 */
inline void write_union_index(std::vector<uint8_t>& buf, uint32_t idx) {
    write_zigzag_long(buf, static_cast<int64_t>(idx));
}

/**
 * Generate a random 16-byte sync marker for Avro container files.
 */
inline std::array<uint8_t, 16> generate_sync_marker() {
    std::array<uint8_t, 16> marker;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 255);
    for (auto& byte : marker) {
        byte = static_cast<uint8_t>(dis(gen));
    }
    return marker;
}

}  // namespace avro_detail

/**
 * Avro container file writer.
 *
 * Writes Avro container files (.avro) with:
 *  - Magic: "Obj\x01"
 *  - Metadata: Avro map<string, bytes> containing "avro.schema" and "avro.codec"
 *  - Sync marker: 16 random bytes
 *  - Data blocks: One block with all records concatenated
 *
 * Usage:
 *   AvroFileWriter writer(schema_json);
 *   std::vector<uint8_t> record1 = ...; // hand-encoded Avro record
 *   writer.append_record(record1);
 *   writer.append_record(record2);
 *   writer.finish("/path/to/file.avro");
 */
class AvroFileWriter {
public:
    /**
     * Construct Avro file writer with a schema.
     * @param schema_json The Avro schema as a JSON string.
     */
    explicit AvroFileWriter(const std::string& schema_json)
        : schema_json_(schema_json),
          sync_marker_(avro_detail::generate_sync_marker()) {}

    /**
     * Append a pre-encoded Avro record to the file.
     * Records must be valid Avro-encoded bytes matching the schema.
     * @param record_bytes The encoded record payload.
     */
    void append_record(const std::vector<uint8_t>& record_bytes) {
        pending_records_.push_back(record_bytes);
    }

    /**
     * Finalize and write the complete Avro container file.
     * After calling finish(), this object becomes invalid; create a new writer for new files.
     * @param output_path Path where the .avro file will be written.
     * @throws std::runtime_error on write errors.
     */
    void finish(const std::string& output_path) {
        std::ofstream out(output_path, std::ios::binary);
        if (!out.is_open()) {
            throw std::runtime_error("Failed to open file for writing: " + output_path);
        }

        try {
            write_header(out);
            write_block(out);
        } catch (...) {
            out.close();
            throw;
        }
        out.close();
    }

    /**
     * Return the number of records pending to be written.
     */
    size_t record_count() const { return pending_records_.size(); }

private:
    std::string schema_json_;
    std::array<uint8_t, 16> sync_marker_;
    std::vector<std::vector<uint8_t>> pending_records_;

    /**
     * Write the Avro container file header:
     *   Magic (4 bytes) + Metadata (map) + Sync marker (16 bytes)
     */
    void write_header(std::ofstream& out) {
        // Magic: "Obj\x01"
        out.put('O');
        out.put('b');
        out.put('j');
        out.put(0x01);

        // Metadata map: {avro.schema, avro.codec}
        auto metadata = encode_metadata_map();
        out.write(reinterpret_cast<const char*>(metadata.data()), metadata.size());

        // Sync marker
        out.write(reinterpret_cast<const char*>(sync_marker_.data()), sync_marker_.size());
    }

    /**
     * Write one data block with all pending records concatenated.
     * Block format (when records present):
     *   [zigzag record-count] [zigzag byte-size] [record1] [record2] ... [sync marker]
     *
     * Note: Per Avro spec, a block record-count of 0 is an end marker with no trailing data.
     * We skip the block entirely for empty records (not writing count=0 with size/sync).
     */
    void write_block(std::ofstream& out) {
        // If no records, don't write any block (Avro spec: 0 count is end marker, no data after)
        if (pending_records_.empty()) {
            return;
        }

        // Calculate total byte size of all records
        size_t total_bytes = 0;
        for (const auto& rec : pending_records_) {
            total_bytes += rec.size();
        }

        // Encode block header
        std::vector<uint8_t> block_header;
        avro_detail::write_zigzag_long(block_header, static_cast<int64_t>(pending_records_.size()));
        avro_detail::write_zigzag_long(block_header, static_cast<int64_t>(total_bytes));
        out.write(reinterpret_cast<const char*>(block_header.data()), block_header.size());

        // Write all records
        for (const auto& rec : pending_records_) {
            out.write(reinterpret_cast<const char*>(rec.data()), rec.size());
        }

        // Write sync marker
        out.write(reinterpret_cast<const char*>(sync_marker_.data()), sync_marker_.size());
    }

    /**
     * Encode metadata as Avro map<string, bytes>:
     *   [count1][key1_str][value1_bytes] [count2][key2_str][value2_bytes] ... [0x00]
     *
     * Two entries:
     *   - "avro.schema" -> schema JSON as bytes
     *   - "avro.codec" -> "null" as bytes
     */
    std::vector<uint8_t> encode_metadata_map() {
        std::vector<uint8_t> map;

        // Entry 1: avro.schema
        avro_detail::write_zigzag_long(map, 1);  // count=1 (one entry in this block)
        avro_detail::write_avro_string(map, "avro.schema");
        avro_detail::write_avro_bytes(map, schema_json_.data(), schema_json_.size());

        // Entry 2: avro.codec
        avro_detail::write_zigzag_long(map, 1);  // count=1
        avro_detail::write_avro_string(map, "avro.codec");
        std::string codec = "null";
        avro_detail::write_avro_bytes(map, codec.data(), codec.size());

        // Terminator: empty block
        avro_detail::write_zigzag_long(map, 0);

        return map;
    }
};

}  // namespace tpch

#endif  // TPCH_AVRO_WRITER_HPP
