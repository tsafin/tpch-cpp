#pragma once

#include <arrow/io/interfaces.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace tpch {

/**
 * Format-agnostic Arrow OutputStream backed by io_uring.
 *
 * Designed for the parallel TPC-DS generation pipeline (DS-10):
 * - One instance per output file, owned by a single child process.
 * - A worker thread owns the io_uring ring for the file's lifetime,
 *   eliminating per-write ring setup overhead.
 * - Write() pre-claims the file offset atomically (fetch_add), copies
 *   data into a WriteJob, enqueues it to the worker, and blocks until
 *   the worker has drained all CQEs — preserving the sequential ordering
 *   that Arrow format writers expect.
 * - Large writes are broken into 512 KB SQEs for pipelining.
 * - No SQPOLL, no O_DIRECT (both hurt on WSL2/VirtIO).
 *
 * Sync fallback: when ring_struct == nullptr (io_uring unavailable),
 * Write() falls back to pwrite(2) with no worker thread.
 *
 * Thread safety: NOT thread-safe (single producer assumed — Arrow writers
 * call Write() from a single thread).
 */
class IoUringOutputStream : public arrow::io::OutputStream {
public:
    /**
     * Open path and prepare the stream.
     *
     * @param path        File to create (O_WRONLY | O_CREAT | O_TRUNC).
     * @param ring_struct Opaque io_uring* from IoUringPool::create_child_ring_struct(),
     *                    or nullptr for synchronous pwrite fallback.
     *                    The stream takes ownership; destructor calls
     *                    IoUringPool::free_ring(ring_struct).
     */
    explicit IoUringOutputStream(const std::string& path,
                                  void* ring_struct = nullptr);
    ~IoUringOutputStream() override;

    // Not copyable or movable (owns file fd + thread)
    IoUringOutputStream(const IoUringOutputStream&) = delete;
    IoUringOutputStream& operator=(const IoUringOutputStream&) = delete;

    // ---- arrow::io::OutputStream ----

    /** Pre-claim offset, copy data, enqueue to worker, wait for completion. */
    arrow::Status Write(const void* data, int64_t nbytes) override;

    /** Convenience overload accepting a Buffer. */
    arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override;

    /** No-op: Write() already blocks until data is on disk. */
    arrow::Status Flush() override;

    /** Stop worker thread (if running), fsync, close file fd. */
    arrow::Status Close() override;

    arrow::Result<int64_t> Tell() const override;
    bool closed() const override;

private:
    // Each Write() call creates one WriteJob.
    struct WriteJob {
        std::vector<uint8_t>        data;
        int64_t                     offset;
        std::promise<arrow::Status> done;
    };

    // Synchronous pwrite path (used when ring_ == nullptr).
    arrow::Status write_sync(const void* data, int64_t nbytes, int64_t offset);

    // Worker thread: pop jobs, submit io_uring SQEs, drain CQEs, signal done.
    void worker_loop();

    int   file_fd_ = -1;
    void* ring_    = nullptr;  // io_uring* or nullptr (sync mode)
    bool  closed_  = false;

    std::atomic<int64_t> write_offset_{0};  // next byte offset to claim

    std::thread             worker_;
    std::mutex              mu_;
    std::condition_variable cv_;
    std::queue<std::unique_ptr<WriteJob>> queue_;
    bool stop_ = false;
};

}  // namespace tpch
