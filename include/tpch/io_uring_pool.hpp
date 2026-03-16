#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace tpch {

/**
 * IoUringPool — dual-role anchor ring manager for parallel TPC-DS generation.
 *
 * Roles:
 *   1. Scheduler (parent): POLL_ADD on child pidfds → CQE per child exit.
 *      Replaces waitpid(-1) with a non-blocking event loop.
 *   2. ATTACH_WQ anchor (children): create_child_ring_struct() returns a new
 *      io_uring ring whose kernel async-worker pool is shared with the anchor.
 *      All child I/O uses one shared kernel thread pool.
 *
 * Usage — parent:
 *   IoUringPool::init(output_dir);
 *   // for each fork():
 *   int pidfd = pidfd_open(child_pid, 0);
 *   IoUringPool::watch_child(pidfd, slot_index);
 *   // to reap:
 *   for (uint64_t idx : IoUringPool::wait_any()) { ... }
 *
 * Usage — child (immediately after fork):
 *   void* ring = IoUringPool::create_child_ring_struct();
 *   auto stream = std::make_shared<IoUringOutputStream>(path, ring);
 *   // stream destructor calls IoUringPool::free_ring(ring)
 *
 * Thread safety: NOT thread-safe. All calls must be from the same thread.
 * (Parent scheduler is single-threaded; children are separate processes.)
 */
class IoUringPool {
public:
    /**
     * Initialise anchor ring. Must be called in parent before any fork().
     * @param output_dir  Path used for sysfs queue-depth calibration.
     * @return true if io_uring is available and anchor was created.
     */
    static bool init(const std::string& output_dir);

    /**
     * (Parent) Submit POLL_ADD on a child pidfd.
     * user_data is returned verbatim in wait_any() when this child exits.
     * Requires pidfd_open(2) (Linux ≥ 5.3).
     */
    static void watch_child(int pidfd, uint64_t user_data);

    /**
     * (Parent) Block until at least one POLL_ADD completes (child exits).
     * Returns user_data values of all completed events in this batch.
     */
    static std::vector<uint64_t> wait_any();

    /**
     * (Child, after fork) Allocate a new io_uring ring attached to the anchor
     * via IORING_SETUP_ATTACH_WQ, so all child I/O shares one kernel thread pool.
     * Falls back to a plain ring if ATTACH_WQ fails.
     * Returns opaque heap-allocated io_uring* (to avoid exposing liburing.h),
     * or nullptr if io_uring is entirely unavailable.
     * Caller should pass this to IoUringOutputStream; the stream owns lifetime.
     */
    static void* create_child_ring_struct();

    /**
     * Release a ring created by create_child_ring_struct().
     * Called by IoUringOutputStream destructor.
     */
    static void free_ring(void* ring_struct);

    /** True if anchor ring was successfully initialised. */
    static bool available();

    /**
     * fd of anchor ring.
     * Useful for Lance FFI ATTACH_WQ (DS-10.4): pass to
     * lance_writer_attach_io_uring_pool() so the Rust side can share the pool.
     */
    static int anchor_fd();

    /** Calibrated queue depth (sysfs nr_requests/2, clamped [8, 128]). */
    static uint32_t queue_depth();

private:
    static void*    anchor_ring_;      // io_uring* cast to void*
    static int      anchor_fd_;        // ring->ring_fd of the anchor
    static uint32_t calibrated_qd_;   // sysfs-calibrated QD
    static bool     available_;
};

}  // namespace tpch
