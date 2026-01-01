#include "tpch/async_io.hpp"

#ifdef TPCH_ENABLE_ASYNC_IO

#include <liburing.h>
#include <stdexcept>
#include <cstring>
#include <sys/types.h>

namespace tpch {

AsyncIOContext::AsyncIOContext(uint32_t queue_depth)
    : queue_depth_(queue_depth), pending_(0) {
    // Allocate io_uring ring structure
    ring_ = new io_uring;

    // Initialize the io_uring ring
    int ret = io_uring_queue_init(queue_depth, static_cast<io_uring*>(ring_), 0);
    if (ret < 0) {
        delete static_cast<io_uring*>(ring_);
        throw std::runtime_error("Failed to initialize io_uring: " + std::string(strerror(-ret)));
    }
}

AsyncIOContext::~AsyncIOContext() {
    try {
        // Flush any pending operations
        flush();

        // Exit the io_uring ring
        io_uring_queue_exit(static_cast<io_uring*>(ring_));

        // Free the ring structure
        delete static_cast<io_uring*>(ring_);
    } catch (...) {
        // Suppress exceptions in destructor
        delete static_cast<io_uring*>(ring_);
    }
}

void AsyncIOContext::submit_write(int fd, const void* buf, size_t count, off_t offset) {
    auto ring = static_cast<io_uring*>(ring_);

    // Get a submission queue entry
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe == nullptr) {
        // Queue is full, wait for some completions
        wait_completions(1);
        sqe = io_uring_get_sqe(ring);
        if (sqe == nullptr) {
            throw std::runtime_error("Failed to get submission queue entry");
        }
    }

    // Prepare the write operation
    io_uring_prep_write(sqe, fd, buf, static_cast<unsigned>(count), offset);

    // Set user data (not used but available for tracking)
    sqe->user_data = 0;

    // Submit to kernel
    int ret = io_uring_submit(ring);
    if (ret < 0) {
        throw std::runtime_error("Failed to submit write operation: " + std::string(strerror(-ret)));
    }

    pending_++;
}

int AsyncIOContext::wait_completions(int min_complete) {
    auto ring = static_cast<io_uring*>(ring_);

    if (pending_ == 0) {
        return 0;
    }

    struct io_uring_cqe* cqe = nullptr;
    int completed = 0;

    // Wait for at least min_complete completions
    int ret = io_uring_wait_cqe_nr(ring, &cqe, min_complete > pending_ ? pending_ : min_complete);
    if (ret < 0) {
        throw std::runtime_error("Failed to wait for completions: " + std::string(strerror(-ret)));
    }

    // Process all available completions
    unsigned head;
    io_uring_for_each_cqe(ring, head, cqe) {
        // Check for errors in completion
        if (cqe->res < 0) {
            throw std::runtime_error("I/O operation failed with error: " + std::string(strerror(-cqe->res)));
        }

        completed++;
        pending_--;
    }

    // Mark completions as seen
    io_uring_cq_advance(ring, completed);

    return completed;
}

int AsyncIOContext::pending_count() const {
    return pending_;
}

void AsyncIOContext::flush() {
    while (pending_ > 0) {
        wait_completions(pending_);
    }
}

}  // namespace tpch

#else

// Stub implementation when TPCH_ENABLE_ASYNC_IO is not defined

namespace tpch {

void AsyncIOContext::submit_write(int fd, const void* buf, size_t count, off_t offset) {
    // Synchronous fallback - perform blocking write
    // Note: This is a simple fallback, not optimal for production
    // In a real system, you'd use regular write() or pwrite()
    (void)fd;      // unused
    (void)buf;     // unused
    (void)count;   // unused
    (void)offset;  // unused
    // Stub implementation - no-op
}

}  // namespace tpch

#endif  // TPCH_ENABLE_ASYNC_IO
