#include "tpch/async_io.hpp"

#ifdef TPCH_ENABLE_ASYNC_IO

#include <liburing.h>
#include <stdexcept>
#include <cstring>
#include <sys/types.h>

namespace tpch {

AsyncIOContext::AsyncIOContext(const AsyncIOConfig& config)
    : queue_depth_(config.queue_depth), pending_(0) {
    // Allocate io_uring ring structure
    ring_ = new io_uring;

    // Setup initialization parameters for SQPOLL if requested
    struct io_uring_params params = {};
    if (config.use_sqpoll) {
        // IORING_SETUP_SQPOLL: kernel thread polls submission queue
        // This reduces syscalls but requires CAP_SYS_NICE capability
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 2000;  // 2 second idle timeout
    }

    // Initialize the io_uring ring with params
    int ret = io_uring_queue_init_params(config.queue_depth,
                                         static_cast<io_uring*>(ring_),
                                         &params);
    if (ret < 0) {
        delete static_cast<io_uring*>(ring_);
        throw std::runtime_error("Failed to initialize io_uring: " + std::string(strerror(-ret)));
    }
}

AsyncIOContext::AsyncIOContext(uint32_t queue_depth)
    : queue_depth_(queue_depth), pending_(0) {
    // Allocate io_uring ring structure
    ring_ = new io_uring;

    // Initialize the io_uring ring with default params (no SQPOLL)
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

int AsyncIOContext::wait_completions(int min_complete, std::vector<uint64_t>* completed_ids) {
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

        // Return which buffers completed
        if (completed_ids) {
            completed_ids->push_back(cqe->user_data);
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

void AsyncIOContext::queue_write(int fd, const void* buf, size_t count, off_t offset, uint64_t user_data) {
    auto ring = static_cast<io_uring*>(ring_);

    // Get a submission queue entry
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe == nullptr) {
        // Queue full - submit current batch first
        submit_queued();
        sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            throw std::runtime_error("Failed to get submission queue entry");
        }
    }

    // Prepare the write operation
    io_uring_prep_write(sqe, fd, buf, static_cast<unsigned>(count), offset);
    sqe->user_data = user_data;  // Track which buffer
    queued_++;
}

int AsyncIOContext::submit_queued() {
    if (queued_ == 0) return 0;

    auto ring = static_cast<io_uring*>(ring_);
    int ret = io_uring_submit(ring);
    if (ret < 0) {
        throw std::runtime_error("Submit failed: " + std::string(strerror(-ret)));
    }

    pending_ += queued_;
    int submitted = queued_;
    queued_ = 0;
    return submitted;
}

int AsyncIOContext::queued_count() const {
    return queued_;
}

void AsyncIOContext::set_completion_callback(CompletionCallback cb) {
    completion_callback_ = cb;
}

int AsyncIOContext::process_completions() {
    auto ring = static_cast<io_uring*>(ring_);
    struct io_uring_cqe* cqe;
    int processed = 0;

    while (io_uring_peek_cqe(ring, &cqe) == 0) {
        if (completion_callback_) {
            completion_callback_(cqe->user_data, cqe->res);
        }
        io_uring_cqe_seen(ring, cqe);
        pending_--;
        processed++;
    }

    return processed;
}

void AsyncIOContext::register_buffers(const std::vector<iovec>& buffers) {
    auto ring = static_cast<io_uring*>(ring_);

    // Register buffers with io_uring kernel
    // The kernel pins these pages, eliminating page table walks during I/O
    int ret = io_uring_register_buffers(ring, buffers.data(), buffers.size());
    if (ret < 0) {
        throw std::runtime_error("Failed to register buffers: " + std::string(strerror(-ret)));
    }

    // Store registered buffers for later reference
    registered_buffers_ = buffers;
}

void AsyncIOContext::queue_write_fixed(int fd, size_t buf_index, size_t count,
                                       off_t offset, uint64_t user_data) {
    if (buf_index >= registered_buffers_.size()) {
        throw std::runtime_error("Buffer index out of range: " + std::to_string(buf_index));
    }

    auto ring = static_cast<io_uring*>(ring_);

    // Get a submission queue entry
    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe == nullptr) {
        // Queue full - submit current batch first
        submit_queued();
        sqe = io_uring_get_sqe(ring);
        if (!sqe) {
            throw std::runtime_error("Failed to get submission queue entry");
        }
    }

    // Prepare write_fixed operation (uses registered buffers)
    // This is zero-copy: kernel already has buffer pages pinned
    io_uring_prep_write_fixed(sqe, fd,
                             registered_buffers_[buf_index].iov_base,
                             count, offset,
                             static_cast<int>(buf_index));
    sqe->user_data = user_data;
    queued_++;
}

bool AsyncIOContext::has_registered_buffers() const {
    return !registered_buffers_.empty();
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

void AsyncIOContext::queue_write(int fd, const void* buf, size_t count, off_t offset, uint64_t user_data) {
    // Stub implementation - no-op
    (void)fd;
    (void)buf;
    (void)count;
    (void)offset;
    (void)user_data;
}

void AsyncIOContext::set_completion_callback(CompletionCallback cb) {
    // Stub implementation - no-op
    (void)cb;
}

int AsyncIOContext::process_completions() {
    // Stub implementation - no-op
    return 0;
}

}  // namespace tpch

#endif  // TPCH_ENABLE_ASYNC_IO
