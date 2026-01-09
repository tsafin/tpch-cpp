#ifndef TPCH_CSV_WRITER_HPP
#define TPCH_CSV_WRITER_HPP

#include <fstream>
#include <memory>
#include <string>
#include <vector>
#include <cstdint>

#include "writer_interface.hpp"

namespace tpch {

class AsyncIOContext;

/**
 * CSV writer implementation using Arrow's CSV API.
 * Writes Arrow RecordBatch data to CSV files with proper escaping and quoting.
 * Supports optional async I/O with io_uring when AsyncIOContext is provided.
 */
class CSVWriter : public WriterInterface {
public:
    /**
     * Create a CSV writer for the specified filepath.
     * The file will be created or overwritten.
     *
     * @param filepath Path to the output CSV file
     */
    explicit CSVWriter(const std::string& filepath);

    ~CSVWriter() override;

    /**
     * Write a batch of rows to the CSV file.
     * The header will be written on the first batch call.
     *
     * @param batch Arrow RecordBatch to write
     */
    void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) override;

    /**
     * Finalize and close the output file.
     */
    void close() override;

    /**
     * Set async I/O context for non-blocking writes.
     *
     * @param context AsyncIOContext for io_uring support
     */
    void set_async_context(std::shared_ptr<AsyncIOContext> context) override;

private:
    std::string filepath_;
    std::ofstream output_;
    int file_descriptor_ = -1;
    bool header_written_ = false;
    std::shared_ptr<AsyncIOContext> async_context_;
    std::vector<uint8_t> write_buffer_;
    static constexpr size_t BUFFER_SIZE = 1024 * 1024;  // 1MB buffer

    /**
     * Write CSV header (field names) to the output.
     *
     * @param batch First batch, used to extract field names from schema
     */
    void write_header(const std::shared_ptr<arrow::RecordBatch>& batch);

    /**
     * Escape a string value for CSV output.
     * Quotes the value if it contains special characters (comma, quote, newline).
     *
     * @param value String to escape
     * @return Escaped string safe for CSV output
     */
    static std::string escape_csv_value(const std::string& value);

    /**
     * Flush buffered data (async or sync).
     */
    void flush_buffer();

    /**
     * Write data directly or buffer for async I/O.
     *
     * @param data Pointer to data
     * @param size Number of bytes
     */
    void write_data(const void* data, size_t size);
};

}  // namespace tpch

#endif  // TPCH_CSV_WRITER_HPP
