#ifndef TPCH_ASYNC_IO_HPP
#define TPCH_ASYNC_IO_HPP

#include <cstddef>
#include <cstdint>
#include <sys/types.h>

#ifdef TPCH_ENABLE_ASYNC_IO

namespace tpch {

/**
 * Asynchronous I/O context using Linux io_uring.
 *
 * Provides efficient batch submission and completion handling for
 * asynchronous write operations. Requires Linux 5.1+ with io_uring support.
 */
class AsyncIOContext {
public:
    /**
     * Initialize async I/O context.
     *
     * @param queue_depth Maximum number of pending requests (default 256)
     */
    explicit AsyncIOContext(uint32_t queue_depth = 256);

    /**
     * Cleanup async I/O context.
     * Waits for all pending operations to complete before destruction.
     */
    ~AsyncIOContext();

    /**
     * Submit an asynchronous write operation.
     *
     * @param fd File descriptor to write to
     * @param buf Pointer to data buffer
     * @param count Number of bytes to write
     * @param offset File offset (ignored for O_APPEND files)
     */
    void submit_write(int fd, const void* buf, size_t count, off_t offset);

    /**
     * Wait for completions.
     *
     * @param min_complete Minimum number of operations to wait for (default 1)
     * @return Number of completed operations
     * @throws std::runtime_error if io_uring operation fails
     */
    int wait_completions(int min_complete = 1);

    /**
     * Get count of pending requests.
     *
     * @return Number of submitted but not yet completed requests
     */
    int pending_count() const;

    /**
     * Flush all pending operations.
     * Waits for all submitted operations to complete.
     */
    void flush();

private:
    // Opaque pointer to io_uring ring structure
    // We use void* to minimize header dependencies
    void* ring_;
    uint32_t queue_depth_;
    int pending_;
};

}  // namespace tpch

#else

// Stub implementation when TPCH_ENABLE_ASYNC_IO is not defined

namespace tpch {

class AsyncIOContext {
public:
    explicit AsyncIOContext(std::uint32_t queue_depth = 256)
        : queue_depth_(queue_depth), pending_(0) {}
    ~AsyncIOContext() = default;

    void submit_write(int fd, const void* buf, std::size_t count, off_t offset);
    int wait_completions(int min_complete = 1) { (void)min_complete; return 0; }
    int pending_count() const { return pending_; }
    void flush() {}

private:
    std::uint32_t queue_depth_;
    int pending_;
};

}  // namespace tpch

#endif  // TPCH_ENABLE_ASYNC_IO

#endif  // TPCH_ASYNC_IO_HPP
