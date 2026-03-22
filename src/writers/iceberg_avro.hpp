// iceberg_avro.hpp – minimal Avro object-container file writer
// Used exclusively by iceberg_writer.cpp.  Do NOT expose in public headers.
//
// Implements just enough of the Avro binary encoding to write the two fixed
// schemas required by Iceberg v1:
//   • manifest_file  (manifest list entries)
//   • manifest_entry (per-data-file entries inside each manifest)
//
// Encoding subset covered:
//   null, int, long, string, bytes, union<null, T>, empty record, empty array
//
// Reference: https://avro.apache.org/docs/1.11.1/specification/
#pragma once

#include <array>
#include <cstdint>
#include <fstream>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace tpch {
namespace avro {

// ─────────────────────────────────────────────────────────────────────────────
// Primitive encoders (append into caller-supplied buffer)
// ─────────────────────────────────────────────────────────────────────────────

// Signed 64-bit → zigzag varint
inline void enc_long(int64_t v, std::vector<uint8_t>& buf) {
    uint64_t n = (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
    do {
        uint8_t b = static_cast<uint8_t>(n & 0x7f);
        n >>= 7;
        if (n) b |= 0x80;
        buf.push_back(b);
    } while (n);
}

// Signed 32-bit → same zigzag varint path
inline void enc_int(int32_t v, std::vector<uint8_t>& buf) {
    enc_long(static_cast<int64_t>(v), buf);
}

// Avro string = long(len) + UTF-8 bytes
inline void enc_string(const std::string& s, std::vector<uint8_t>& buf) {
    enc_long(static_cast<int64_t>(s.size()), buf);
    buf.insert(buf.end(), s.begin(), s.end());
}

// Avro bytes = long(len) + raw bytes
inline void enc_bytes(const void* data, size_t len, std::vector<uint8_t>& buf) {
    enc_long(static_cast<int64_t>(len), buf);
    const auto* p = static_cast<const uint8_t*>(data);
    buf.insert(buf.end(), p, p + len);
}

// union[null, T]: discriminant 0 → null (no further bytes)
inline void enc_null_union(std::vector<uint8_t>& buf) {
    enc_long(0, buf);
}

// union[null, long]: discriminant 1 → long value
inline void enc_union_long(int64_t v, std::vector<uint8_t>& buf) {
    enc_long(1, buf);
    enc_long(v, buf);
}

// union[null, int]: discriminant 1 → int value
inline void enc_union_int(int32_t v, std::vector<uint8_t>& buf) {
    enc_long(1, buf);
    enc_int(v, buf);
}

// ─────────────────────────────────────────────────────────────────────────────
// Avro object-container file writer
// ─────────────────────────────────────────────────────────────────────────────

// Usage:
//   FileWriter fw(schema_json_string);
//   fw.add_record(record_bytes);   // repeat for each object
//   fw.write_to_file("/path/to/file.avro");

class FileWriter {
public:
    explicit FileWriter(std::string schema_json)
        : schema_json_(std::move(schema_json)) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<unsigned> dis(0, 255);
        for (auto& b : sync_marker_) b = static_cast<uint8_t>(dis(gen));
    }

    // Add one fully-encoded record (must match the schema passed to constructor).
    void add_record(std::vector<uint8_t> record) {
        records_.push_back(std::move(record));
    }

    // Serialise to an Avro container file.
    // Format: magic | file-metadata (map) | sync | [data-block | sync]
    void write_to_file(const std::string& path) const {
        std::ofstream f(path, std::ios::binary | std::ios::trunc);
        if (!f.is_open())
            throw std::runtime_error("avro::FileWriter: cannot open " + path);

        // Magic bytes
        f.write("Obj\x01", 4);

        // File metadata: Avro map<string,bytes>  (2 entries)
        {
            std::vector<uint8_t> meta;
            enc_long(2, meta);                           // block count = 2

            enc_string("avro.codec", meta);
            enc_bytes("null", 4, meta);

            enc_string("avro.schema", meta);
            enc_bytes(schema_json_.data(), schema_json_.size(), meta);

            enc_long(0, meta);                           // end of map
            f.write(reinterpret_cast<const char*>(meta.data()),
                    static_cast<std::streamsize>(meta.size()));
        }

        // Header sync marker
        f.write(reinterpret_cast<const char*>(sync_marker_.data()), 16);

        // One data block for all records
        if (!records_.empty()) {
            size_t total_bytes = 0;
            for (const auto& r : records_) total_bytes += r.size();

            std::vector<uint8_t> blk_hdr;
            enc_long(static_cast<int64_t>(records_.size()), blk_hdr);
            enc_long(static_cast<int64_t>(total_bytes),     blk_hdr);
            f.write(reinterpret_cast<const char*>(blk_hdr.data()),
                    static_cast<std::streamsize>(blk_hdr.size()));

            for (const auto& r : records_)
                f.write(reinterpret_cast<const char*>(r.data()),
                        static_cast<std::streamsize>(r.size()));

            // Per-block sync marker
            f.write(reinterpret_cast<const char*>(sync_marker_.data()), 16);
        }
    }

private:
    std::string                       schema_json_;
    std::array<uint8_t, 16>           sync_marker_{};
    std::vector<std::vector<uint8_t>> records_;
};

}  // namespace avro
}  // namespace tpch
