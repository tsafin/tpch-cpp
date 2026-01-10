#ifndef TPCH_MULTI_TABLE_WRITER_HPP
#define TPCH_MULTI_TABLE_WRITER_HPP

#include "writer_interface.hpp"
#include "dbgen_wrapper.hpp"
#include <memory>
#include <vector>
#include <unordered_map>
#include <string>

namespace tpch {

// Forward declarations
class SharedAsyncIOContext;

/**
 * Coordinator for writing multiple TPC-H tables concurrently with async I/O.
 *
 * This class manages multiple writer instances (one per table) and provides
 * a unified interface for batching writes to all tables. It's designed to work
 * with SharedAsyncIOContext to enable concurrent file I/O.
 *
 * Usage example:
 * ```
 * MultiTableWriter writer("output_dir", "parquet");
 *
 * writer.start_tables({TableType::LINEITEM, TableType::ORDERS, TableType::CUSTOMER});
 *
 * // Generate and write data for each table
 * auto batch = ...; // Arrow RecordBatch
 * writer.write_batch(TableType::LINEITEM, batch);
 * writer.write_batch(TableType::ORDERS, batch);
 *
 * writer.finish_all();
 * ```
 */
class MultiTableWriter {
public:
    /**
     * Create a multi-table writer.
     *
     * @param output_dir Directory where output files will be written
     * @param format Output format ("csv", "parquet", "orc")
     * @param use_async_io Enable async I/O for concurrent writes (default: true)
     */
    MultiTableWriter(const std::string& output_dir, const std::string& format,
                     bool use_async_io = true);

    ~MultiTableWriter();

    /**
     * Initialize writers for specified tables.
     * Creates output files and prepares write infrastructure.
     *
     * @param tables Vector of table types to write
     */
    void start_tables(const std::vector<TableType>& tables);

    /**
     * Write a batch to a specific table.
     *
     * @param table_type The table type
     * @param batch Arrow RecordBatch to write
     */
    void write_batch(TableType table_type, const std::shared_ptr<arrow::RecordBatch>& batch);

    /**
     * Finalize all tables.
     * Flushes pending I/O and closes all files.
     */
    void finish_all();

    /**
     * Get writer for a specific table (for direct access if needed).
     *
     * @param table_type The table type
     * @return Pointer to the WriterInterface (may be null if table not started)
     */
    WriterInterface* get_writer(TableType table_type);

    /**
     * Get the shared async I/O context.
     * Useful for monitoring I/O progress or direct I/O operations.
     *
     * @return Shared pointer to async context (may be null if async disabled)
     */
    std::shared_ptr<SharedAsyncIOContext> get_async_context() const;

    /**
     * Get total pending I/O operations across all tables.
     *
     * @return Number of pending async I/O operations
     */
    int pending_io_count() const;

    /**
     * Enable or disable async I/O (affects subsequent write_batch calls).
     *
     * @param enabled true to enable async I/O
     */
    void set_async_io_enabled(bool enabled);

private:
    struct TableWriter {
        WriterPtr writer;      // Actual writer implementation
        TableType table_type;  // Associated table type
        bool initialized = false;
    };

    std::string output_dir_;
    std::string format_;
    bool use_async_io_;
    std::unordered_map<int, TableWriter> table_writers_;
    std::shared_ptr<SharedAsyncIOContext> async_ctx_;

    /**
     * Create output filepath for a table.
     */
    std::string get_table_filename(TableType table_type) const;

    /**
     * Create a writer for a specific table and format.
     */
    WriterPtr create_writer(TableType table_type, const std::string& filepath);
};

}  // namespace tpch

#endif  // TPCH_MULTI_TABLE_WRITER_HPP
