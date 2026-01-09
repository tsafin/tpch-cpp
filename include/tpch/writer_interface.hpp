#ifndef TPCH_WRITER_INTERFACE_HPP
#define TPCH_WRITER_INTERFACE_HPP

#include <memory>
#include <arrow/record_batch.h>

namespace tpch {

// Forward declaration
class AsyncIOContext;

/**
 * Abstract base class for output format writers.
 * Implementations handle writing Arrow RecordBatch data to specific formats.
 */
class WriterInterface {
public:
    virtual ~WriterInterface() = default;

    /**
     * Write a batch of rows (Arrow RecordBatch) to the output.
     */
    virtual void write_batch(const std::shared_ptr<arrow::RecordBatch>& batch) = 0;

    /**
     * Finalize and close the output.
     */
    virtual void close() = 0;

    /**
     * Set async I/O context (optional).
     * Called before write_batch if async I/O is enabled.
     */
    virtual void set_async_context(std::shared_ptr<AsyncIOContext> context) {
        (void)context;  // Default: ignore async context
    }
};

using WriterPtr = std::unique_ptr<WriterInterface>;

}  // namespace tpch

#endif  // TPCH_WRITER_INTERFACE_HPP
