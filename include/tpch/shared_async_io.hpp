#ifndef TPCH_SHARED_ASYNC_IO_HPP
#define TPCH_SHARED_ASYNC_IO_HPP

#include "async_io.hpp"
#include <cstddef>
#include <cstdint>
#include <sys/types.h>
#include <memory>
#include <vector>
#include <unordered_map>
#include <string>

namespace tpch {

/**
 * Shared async I/O context for multiple concurrent file writes.
 *
 * This class manages a single io_uring ring for writing to multiple file
 * descriptors concurrently. This is the primary use case where async I/O
 * provides significant benefits (2-4x speedup compared to sequential writes).
 *
 * Usage example:
 * ```
 * SharedAsyncIOContext async_ctx(512);
 *
 * int fd1 = async_ctx.register_file("output1.parquet");
 * int fd2 = async_ctx.register_file("output2.parquet");
 *
 * async_ctx.queue_write(fd1, data1, size1, 0);
 * async_ctx.queue_write(fd2, data2, size2, 0);
 * async_ctx.submit_all();
 *
 * async_ctx.wait_any(1);
 * ```
 */
class SharedAsyncIOContext {
public:
    /**
     * Create a shared async I/O context.
     *
     * @param queue_depth Maximum number of pending I/O operations across all files
     */
    explicit SharedAsyncIOContext(size_t queue_depth = 256);

    /**
     * Create with custom async I/O configuration.
     *
     * @param config AsyncIOConfig for detailed tuning
     */
    explicit SharedAsyncIOContext(const AsyncIOConfig& config);

    ~SharedAsyncIOContext() = default;

    /**
     * Register a file for async operations.
     * Opens the file for writing, creating it if necessary.
     *
     * @param path File path to open/create
     * @return File handle (used for subsequent operations)
     * @throws std::runtime_error if file cannot be opened
     */
    int register_file(const std::string& path);

    /**
     * Queue a write operation to a registered file.
     * Does not perform actual I/O until submit_all() is called.
     *
     * @param file_handle File handle from register_file()
     * @param buf Data to write
     * @param count Number of bytes
     * @param offset File offset (automatically advanced by this class)
     */
    void queue_write(int file_handle, const void* buf, size_t count);

    /**
     * Submit all queued write operations across all files.
     * After this call, operations are submitted to the kernel.
     *
     * @return Number of operations submitted
     */
    int submit_all();

    /**
     * Wait for at least one completion from any file.
     * Blocks until the specified number of operations complete.
     *
     * @param min_complete Minimum operations to wait for (default 1)
     * @return Number of completions that occurred
     */
    int wait_any(int min_complete = 1);

    /**
     * Wait for all pending operations to complete.
     * Blocks until no pending operations remain.
     */
    void flush();

    /**
     * Get count of pending operations across all files.
     *
     * @return Total pending I/O operations
     */
    int pending_count() const;

    /**
     * Close a file and clean up its resources.
     *
     * @param file_handle File handle to close
     */
    void close_file(int file_handle);

    /**
     * Close all registered files.
     * Automatically waits for pending operations to complete.
     */
    void close_all();

    /**
     * Get current file size (file offset for the next write).
     *
     * @param file_handle File handle
     * @return Current write offset for this file
     */
    off_t get_offset(int file_handle) const;

private:
    struct FileState {
        int fd;                 // Linux file descriptor
        std::string path;       // File path
        off_t offset = 0;       // Current write offset
    };

    std::shared_ptr<AsyncIOContext> async_ctx_;  // Shared io_uring ring
    std::unordered_map<int, FileState> files_;   // Registered files
    int next_file_handle_ = 1;                    // Counter for file handles
};

}  // namespace tpch

#endif  // TPCH_SHARED_ASYNC_IO_HPP
