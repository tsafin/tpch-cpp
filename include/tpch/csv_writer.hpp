#ifndef TPCH_CSV_WRITER_HPP
#define TPCH_CSV_WRITER_HPP

#include <memory>
#include <string>
#include <vector>
#include <array>
#include <bitset>
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
     * @param use_direct_io Enable O_DIRECT for bypassing page cache (default: false)
     */
    explicit CSVWriter(const std::string& filepath, bool use_direct_io = false);

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

    /**
     * Enable or disable O_DIRECT mode (must be called before first write).
     * O_DIRECT bypasses page cache for direct disk writes, improving throughput for large sequential writes.
     *
     * @param enable Enable O_DIRECT (requires buffer alignment)
     */
    void enable_direct_io(bool enable);

private:
    std::string filepath_;
    int file_descriptor_ = -1;
    bool header_written_ = false;
    std::shared_ptr<AsyncIOContext> async_context_;
    static constexpr size_t BUFFER_SIZE = 1024 * 1024;  // 1MB buffer
    static constexpr size_t NUM_BUFFERS = 8;  // Match reasonable queue depth
    static constexpr size_t ALIGNMENT = 4096;  // O_DIRECT alignment requirement

    // O_DIRECT support
    bool use_direct_io_ = false;

    // Buffer pool for async I/O
    std::array<std::vector<uint8_t>, NUM_BUFFERS> buffer_pool_;
    std::bitset<NUM_BUFFERS> buffer_in_flight_;  // Track which are pending
    size_t current_buffer_idx_ = 0;
    size_t buffer_fill_size_ = 0;  // Current fill level of active buffer
    off_t current_offset_ = 0;  // Track cumulative file position

    /**
     * Initialize aligned buffers for O_DIRECT if enabled.
     */
    void init_aligned_buffers();

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

    /**
     * Acquire a free buffer from the pool.
     * Waits for completion if all buffers are in-flight.
     *
     * @return Index of free buffer
     */
    size_t acquire_buffer();

    /**
     * Wait for at least one in-flight operation to complete.
     */
    void wait_for_completion();

    /**
     * Release a buffer back to the pool (mark as not in-flight).
     *
     * @param idx Buffer index
     */
    void release_buffer(size_t idx);
};

}  // namespace tpch

#endif  // TPCH_CSV_WRITER_HPP
