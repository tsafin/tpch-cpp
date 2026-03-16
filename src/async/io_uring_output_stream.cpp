#include "tpch/io_uring_output_stream.hpp"
#include "tpch/io_uring_pool.hpp"

#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <sys/types.h>
#include <unistd.h>

#include <arrow/buffer.h>

#ifdef TPCH_ENABLE_ASYNC_IO
#include <liburing.h>
#endif

namespace tpch {

// ---------------------------------------------------------------------------
// Constructor / Destructor
// ---------------------------------------------------------------------------

IoUringOutputStream::IoUringOutputStream(const std::string& path,
                                          void* ring_struct)
    : ring_(ring_struct) {
    file_fd_ = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (file_fd_ < 0) {
        throw std::runtime_error(
            "IoUringOutputStream: cannot open '" + path +
            "': " + strerror(errno));
    }

#ifdef TPCH_ENABLE_ASYNC_IO
    if (ring_ != nullptr) {
        worker_ = std::thread([this] { worker_loop(); });
    }
#endif
}

IoUringOutputStream::~IoUringOutputStream() {
    if (!closed_) {
        (void)Close();
    }
    IoUringPool::free_ring(ring_);
    ring_ = nullptr;
}

// ---------------------------------------------------------------------------
// OutputStream interface
// ---------------------------------------------------------------------------

arrow::Result<int64_t> IoUringOutputStream::Tell() const {
    return write_offset_.load(std::memory_order_relaxed);
}

bool IoUringOutputStream::closed() const { return closed_; }

arrow::Status IoUringOutputStream::Write(const void* data, int64_t nbytes) {
    if (closed_)
        return arrow::Status::IOError("IoUringOutputStream: stream is closed");
    if (nbytes <= 0) return arrow::Status::OK();

    // Pre-claim offset atomically so multiple future async writes don't overlap
    int64_t off =
        write_offset_.fetch_add(nbytes, std::memory_order_relaxed);

#ifdef TPCH_ENABLE_ASYNC_IO
    if (ring_ != nullptr) {
        auto job    = std::make_unique<WriteJob>();
        job->data.assign(static_cast<const uint8_t*>(data),
                         static_cast<const uint8_t*>(data) + nbytes);
        job->offset = off;
        auto fut    = job->done.get_future();

        {
            std::lock_guard<std::mutex> lk(mu_);
            queue_.push(std::move(job));
        }
        cv_.notify_one();

        // Block until worker has submitted + drained all CQEs for this job.
        // Preserves the sequential write ordering that Arrow writers expect.
        return fut.get();
    }
#endif

    return write_sync(data, nbytes, off);
}

arrow::Status IoUringOutputStream::Write(
    const std::shared_ptr<arrow::Buffer>& data) {
    return Write(data->data(), data->size());
}

// Write() already blocks until the worker drains CQEs, so Flush is a no-op.
arrow::Status IoUringOutputStream::Flush() { return arrow::Status::OK(); }

arrow::Status IoUringOutputStream::Close() {
    if (closed_) return arrow::Status::OK();
    closed_ = true;

#ifdef TPCH_ENABLE_ASYNC_IO
    if (ring_ != nullptr && worker_.joinable()) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_one();
        worker_.join();
    }
#endif

    if (file_fd_ >= 0) {
        ::close(file_fd_);
        file_fd_ = -1;
    }
    return arrow::Status::OK();
}

// ---------------------------------------------------------------------------
// Synchronous fallback (ring_ == nullptr)
// ---------------------------------------------------------------------------

arrow::Status IoUringOutputStream::write_sync(const void* data, int64_t nbytes,
                                               int64_t offset) {
    const uint8_t* ptr     = static_cast<const uint8_t*>(data);
    int64_t        remaining = nbytes;
    while (remaining > 0) {
        ssize_t n =
            pwrite(file_fd_, ptr, static_cast<size_t>(remaining), offset);
        if (n < 0) {
            if (errno == EINTR) continue;
            return arrow::Status::IOError("IoUringOutputStream: pwrite: ",
                                          strerror(errno));
        }
        ptr       += n;
        offset    += n;
        remaining -= n;
    }
    return arrow::Status::OK();
}

// ---------------------------------------------------------------------------
// io_uring worker thread
// ---------------------------------------------------------------------------

#ifdef TPCH_ENABLE_ASYNC_IO

void IoUringOutputStream::worker_loop() {
    static constexpr size_t CHUNK_SIZE = 512UL * 1024;  // 512 KB per SQE
    auto* ring = static_cast<io_uring*>(ring_);

    while (true) {
        // --- wait for a job ---
        std::unique_ptr<WriteJob> job;
        {
            std::unique_lock<std::mutex> lk(mu_);
            cv_.wait(lk, [this] { return !queue_.empty() || stop_; });
            if (queue_.empty()) break;  // stop_ set and queue drained
            job = std::move(queue_.front());
            queue_.pop();
        }

        const uint8_t* ptr    = job->data.data();
        size_t         rem    = job->data.size();
        int64_t        off    = job->offset;
        int            inflight = 0;
        arrow::Status  st     = arrow::Status::OK();

        // --- fill SQ with 512 KB chunks ---
        while (rem > 0) {
            size_t chunk = std::min(CHUNK_SIZE, rem);

            struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
            if (!sqe) {
                // SQ full: submit what we have, wait for one CQE to free a slot
                io_uring_submit(ring);
                struct io_uring_cqe* cqe = nullptr;
                int r = io_uring_wait_cqe(ring, &cqe);
                if (r == 0) {
                    if (cqe->res < 0 && st.ok())
                        st = arrow::Status::IOError(
                            "io_uring write: ", strerror(-cqe->res));
                    io_uring_cqe_seen(ring, cqe);
                    --inflight;
                }
                sqe = io_uring_get_sqe(ring);
                if (!sqe) {
                    if (st.ok())
                        st = arrow::Status::IOError(
                            "io_uring: cannot get SQE after flush");
                    break;
                }
            }

            io_uring_prep_write(sqe, file_fd_, ptr,
                                static_cast<unsigned>(chunk),
                                static_cast<off_t>(off));
            sqe->user_data = 0;
            ++inflight;

            ptr  += chunk;
            off  += static_cast<int64_t>(chunk);
            rem  -= chunk;
        }

        // --- submit remainder + drain all in-flight CQEs ---
        if (inflight > 0) {
            io_uring_submit(ring);
            while (inflight > 0) {
                struct io_uring_cqe* cqe = nullptr;
                int r = io_uring_wait_cqe(ring, &cqe);
                if (r == 0) {
                    if (cqe->res < 0 && st.ok())
                        st = arrow::Status::IOError(
                            "io_uring write: ", strerror(-cqe->res));
                    io_uring_cqe_seen(ring, cqe);
                }
                --inflight;
            }
        }

        job->done.set_value(st);
    }
}

#endif  // TPCH_ENABLE_ASYNC_IO

}  // namespace tpch
