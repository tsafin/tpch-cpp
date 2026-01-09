#ifndef TPCH_ASYNC_IO_HPP
#define TPCH_ASYNC_IO_HPP

#include <cstddef>
#include <cstdint>
#include <sys/types.h>
#include <vector>
#include <functional>

#ifdef TPCH_ENABLE_ASYNC_IO

namespace tpch {

/**
 * Callback function for handling I/O completions.
 * Called with the user_data associated with the operation and the completion result.
 */
using CompletionCallback = std::function<void(uint64_t user_data, int result)>;

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
     * @param completed_ids Optional vector to receive user_data values of completed operations
     * @return Number of completed operations
     * @throws std::runtime_error if io_uring operation fails
     */
    int wait_completions(int min_complete = 1, std::vector<uint64_t>* completed_ids = nullptr);

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

    /**
     * Queue a write operation without immediate submission.
     *
     * @param fd File descriptor to write to
     * @param buf Pointer to data buffer
     * @param count Number of bytes to write
     * @param offset File offset
     * @param user_data User data to associate with this operation (for tracking)
     */
    void queue_write(int fd, const void* buf, size_t count, off_t offset, uint64_t user_data);

    /**
     * Submit all queued operations to the kernel.
     *
     * @return Number of operations submitted
     */
    int submit_queued();

    /**
     * Get count of queued but not yet submitted operations.
     *
     * @return Number of queued operations
     */
    int queued_count() const;

    /**
     * Set callback for completion events.
     *
     * @param cb Callback function to invoke for each completion
     */
    void set_completion_callback(CompletionCallback cb);

    /**
     * Process all available completions and invoke callbacks.
     * Non-blocking - returns immediately if no completions are available.
     *
     * @return Number of completions processed
     */
    int process_completions();

private:
    // Opaque pointer to io_uring ring structure
    // We use void* to minimize header dependencies
    void* ring_;
    uint32_t queue_depth_;
    int pending_;
    int queued_ = 0;  // Number of SQEs prepared but not submitted
    CompletionCallback completion_callback_;
};

}  // namespace tpch

#else

// Stub implementation when TPCH_ENABLE_ASYNC_IO is not defined

namespace tpch {

class AsyncIOContext {
public:
    explicit AsyncIOContext(std::uint32_t queue_depth = 256)
        : queue_depth_(queue_depth), pending_(0), queued_(0) {}
    ~AsyncIOContext() = default;

    void submit_write(int fd, const void* buf, std::size_t count, off_t offset);
    int wait_completions(int min_complete = 1, std::vector<uint64_t>* completed_ids = nullptr) {
        (void)min_complete;
        (void)completed_ids;
        return 0;
    }
    int pending_count() const { return pending_; }
    void flush() {}
    void queue_write(int fd, const void* buf, std::size_t count, off_t offset, std::uint64_t user_data);
    int submit_queued() { return 0; }
    int queued_count() const { return queued_; }
    void set_completion_callback(CompletionCallback cb) { (void)cb; }
    int process_completions() { return 0; }

private:
    std::uint32_t queue_depth_;
    int pending_;
    int queued_;
};

}  // namespace tpch

#endif  // TPCH_ENABLE_ASYNC_IO

#endif  // TPCH_ASYNC_IO_HPP
