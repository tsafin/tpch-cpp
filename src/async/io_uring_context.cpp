#include "tpch/async_io.hpp"

#ifdef TPCH_ENABLE_ASYNC_IO

#include <liburing.h>
#include <stdexcept>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>
#include <climits>
#include <cstdio>
#include <algorithm>
#include <mutex>
#include <string>

namespace tpch {

// ── Sysfs queue-depth calibration ────────────────────────────────────────────
//
// Mirror of the Rust sysfs_calibrate_qd() in io_uring_store.rs:
//   1. stat(path_or_ancestor) → st_dev → major:minor
//   2. realpath(/sys/dev/block/MAJOR:MINOR) → canonical sysfs dir
//   3. Walk up looking for queue/nr_requests
//   4. Return nr_requests/2 clamped to [8, 128]; fallback 64

static uint32_t sysfs_calibrate_qd(const char* path) {
    struct stat st;

    // Walk up to an existing ancestor (target file may not exist yet).
    std::string p = path ? path : "/";
    while (!p.empty() && stat(p.c_str(), &st) != 0) {
        size_t pos = p.rfind('/');
        if (pos == 0) { p = "/"; break; }
        if (pos == std::string::npos) break;
        p.resize(pos);
    }
    if (stat(p.c_str(), &st) != 0) return 64;

    // Build /sys/dev/block/MAJOR:MINOR symlink path.
    char link[256];
    snprintf(link, sizeof(link), "/sys/dev/block/%u:%u",
             major(st.st_dev), minor(st.st_dev));

    char canon[PATH_MAX];
    if (realpath(link, canon) == nullptr) return 64;

    // Walk up from canonical sysfs path to find queue/nr_requests.
    std::string cur = canon;
    while (cur.size() > 1) {
        std::string nr_req_path = cur + "/queue/nr_requests";
        FILE* f = fopen(nr_req_path.c_str(), "r");
        if (f) {
            unsigned int n = 0;
            fscanf(f, "%u", &n);
            fclose(f);
            if (n > 0) {
                uint32_t qd = static_cast<uint32_t>(n) / 2;
                if (qd <   8) qd =   8;
                if (qd > 128) qd = 128;
                return qd;
            }
        }
        size_t slash = cur.rfind('/');
        if (slash == std::string::npos || slash == 0) break;
        cur.resize(slash);
    }
    return 64;  // fallback: /tmp or other pseudo-fs
}

// ── Process-global anchor ring (IORING_SETUP_ATTACH_WQ) ─────────────────────
//
// All rings after the first one attach to this anchor's kernel async-worker
// thread pool, reducing scheduler pressure on WSL2/Hyper-V IOThread.
// The anchor io_uring is intentionally leaked (never freed) so its fd stays
// open for the process lifetime.

static io_uring* g_anchor_ring = nullptr;
static int       g_anchor_fd   = -1;
static std::once_flag g_anchor_once;

static void init_anchor(uint32_t qd) {
    g_anchor_ring = new io_uring;
    if (io_uring_queue_init(qd, g_anchor_ring, 0) == 0) {
        g_anchor_fd = g_anchor_ring->ring_fd;
    } else {
        delete g_anchor_ring;
        g_anchor_ring = nullptr;
        g_anchor_fd   = -1;
    }
}

static int get_anchor_fd(uint32_t qd) {
    std::call_once(g_anchor_once, init_anchor, qd);
    return g_anchor_fd;
}

AsyncIOContext::AsyncIOContext(const AsyncIOConfig& config)
    : queue_depth_(config.queue_depth), pending_(0) {
    ring_ = new io_uring;

    struct io_uring_params params = {};
    if (config.use_sqpoll) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 2000;
    }

    // Attach to shared kernel worker pool (mirrors Rust IORING_SETUP_ATTACH_WQ).
    // Skip when SQPOLL is requested (incompatible combination).
    int anchor = config.use_sqpoll ? -1 : get_anchor_fd(config.queue_depth);
    if (anchor >= 0) {
        params.flags |= IORING_SETUP_ATTACH_WQ;
        params.wq_fd   = static_cast<uint32_t>(anchor);
    }

    int ret = io_uring_queue_init_params(config.queue_depth,
                                         static_cast<io_uring*>(ring_),
                                         &params);
    if (ret < 0) {
        // Retry without ATTACH_WQ on kernels that don't support it.
        params.flags &= ~static_cast<unsigned>(IORING_SETUP_ATTACH_WQ);
        ret = io_uring_queue_init_params(config.queue_depth,
                                          static_cast<io_uring*>(ring_),
                                          &params);
    }
    if (ret < 0) {
        delete static_cast<io_uring*>(ring_);
        throw std::runtime_error("Failed to initialize io_uring: " + std::string(strerror(-ret)));
    }
}

AsyncIOContext::AsyncIOContext(uint32_t queue_depth)
    : queue_depth_(queue_depth), pending_(0) {
    ring_ = new io_uring;

    // Attach to shared kernel worker pool (mirrors Rust IORING_SETUP_ATTACH_WQ).
    int anchor = get_anchor_fd(queue_depth);
    int ret = -1;
    if (anchor >= 0) {
        struct io_uring_params params = {};
        params.flags = IORING_SETUP_ATTACH_WQ;
        params.wq_fd  = static_cast<uint32_t>(anchor);
        ret = io_uring_queue_init_params(queue_depth,
                                          static_cast<io_uring*>(ring_),
                                          &params);
    }
    if (ret < 0) {
        // Fallback: plain ring without shared worker pool.
        ret = io_uring_queue_init(queue_depth, static_cast<io_uring*>(ring_), 0);
    }
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
    const uint8_t* byte_buf = static_cast<const uint8_t*>(buf);

    // Split large writes into chunks (max 2GB per io_uring operation to avoid 32-bit truncation)
    // io_uring_prep_write() takes unsigned (32-bit), so we split at 2GB to be safe
    static constexpr size_t MAX_CHUNK_SIZE = 2ULL * 1024 * 1024 * 1024;  // 2GB

    size_t written = 0;
    while (written < count) {
        size_t chunk_size = std::min(MAX_CHUNK_SIZE, count - written);

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

        // Prepare the write operation (safe cast - chunk_size is at most 2GB)
        io_uring_prep_write(sqe, fd, byte_buf + written,
                           static_cast<unsigned>(chunk_size),
                           offset + written);

        // Set user data (not used but available for tracking)
        sqe->user_data = 0;

        written += chunk_size;
    }

    // Submit all chunks to kernel
    int ret = io_uring_submit(ring);
    if (ret < 0) {
        throw std::runtime_error("Failed to submit write operation: " + std::string(strerror(-ret)));
    }

    pending_ += (count + MAX_CHUNK_SIZE - 1) / MAX_CHUNK_SIZE;  // Count number of chunks
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
    const uint8_t* byte_buf = static_cast<const uint8_t*>(buf);

    // Split large writes into chunks (max 2GB per io_uring operation to avoid 32-bit truncation)
    static constexpr size_t MAX_CHUNK_SIZE = 2ULL * 1024 * 1024 * 1024;  // 2GB

    size_t written = 0;
    while (written < count) {
        size_t chunk_size = std::min(MAX_CHUNK_SIZE, count - written);

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

        // Prepare the write operation (safe cast - chunk_size is at most 2GB)
        io_uring_prep_write(sqe, fd, byte_buf + written,
                           static_cast<unsigned>(chunk_size),
                           offset + written);
        sqe->user_data = user_data;  // Track which buffer
        queued_++;

        written += chunk_size;
    }
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

int AsyncIOContext::submit_and_wait(int min_complete) {
    if (queued_ == 0 && pending_ == 0) return 0;

    auto ring = static_cast<io_uring*>(ring_);
    int to_submit = queued_;
    int to_wait   = (min_complete < pending_ + queued_) ? min_complete : (pending_ + queued_);
    if (to_wait < 1) to_wait = 1;

    // Single syscall: submit all prepared SQEs + wait for to_wait CQEs.
    int ret = io_uring_submit_and_wait(ring, static_cast<unsigned>(to_wait));
    if (ret < 0) {
        throw std::runtime_error("io_uring submit_and_wait failed: " + std::string(strerror(-ret)));
    }
    pending_ += to_submit;
    queued_   = 0;

    // Drain all available CQEs (may be more than min_complete).
    struct io_uring_cqe* cqe = nullptr;
    unsigned head;
    int completed = 0;
    io_uring_for_each_cqe(ring, head, cqe) {
        if (cqe->res < 0) {
            throw std::runtime_error("I/O error in submit_and_wait: " + std::string(strerror(-cqe->res)));
        }
        completed++;
    }
    io_uring_cq_advance(ring, completed);
    pending_ -= completed;
    return completed;
}

// static
uint32_t AsyncIOContext::calibrate_queue_depth(const char* path) {
    return sysfs_calibrate_qd(path);
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
