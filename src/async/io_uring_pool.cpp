#include "tpch/io_uring_pool.hpp"

#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/sysmacros.h>

#ifdef TPCH_ENABLE_ASYNC_IO
#include <liburing.h>
#include <poll.h>  // POLLIN
#endif

namespace tpch {

// Static member definitions
void*    IoUringPool::anchor_ring_   = nullptr;
int      IoUringPool::anchor_fd_     = -1;
uint32_t IoUringPool::calibrated_qd_ = 32;
bool     IoUringPool::available_     = false;

// ---- real implementation ------------------------------------------------
#ifdef TPCH_ENABLE_ASYNC_IO

static uint32_t sysfs_queue_depth(const std::string& dir) {
    struct stat st;
    if (stat(dir.c_str(), &st) != 0) return 32;

    unsigned dev_major     = major(st.st_dev);
    unsigned dev_minor_val = minor(st.st_dev);

    DIR* d = opendir("/sys/block");
    if (!d) return 32;

    uint32_t result = 32;
    struct dirent* ent;
    while ((ent = readdir(d)) != nullptr) {
        if (ent->d_name[0] == '.') continue;

        char dev_path[512];
        snprintf(dev_path, sizeof(dev_path), "/sys/block/%s/dev", ent->d_name);

        FILE* f = fopen(dev_path, "r");
        if (!f) continue;

        unsigned maj = 0, min_val = 0;
        bool matched = (fscanf(f, "%u:%u", &maj, &min_val) == 2 &&
                        maj == dev_major && min_val == dev_minor_val);
        fclose(f);
        if (!matched) continue;

        char nr_path[512];
        snprintf(nr_path, sizeof(nr_path),
                 "/sys/block/%s/queue/nr_requests", ent->d_name);
        FILE* nf = fopen(nr_path, "r");
        if (nf) {
            unsigned nr = 0;
            if (fscanf(nf, "%u", &nr) == 1 && nr > 0) {
                result = nr / 2;
                if (result < 8)   result = 8;
                if (result > 128) result = 128;
            }
            fclose(nf);
        }
        break;
    }
    closedir(d);
    return result;
}

bool IoUringPool::init(const std::string& output_dir) {
    if (available_) return true;

    calibrated_qd_ = sysfs_queue_depth(output_dir);

    auto* ring = new io_uring{};
    int ret = io_uring_queue_init(calibrated_qd_, ring, 0);
    if (ret < 0) {
        delete ring;
        fprintf(stderr,
                "IoUringPool: io_uring_queue_init(QD=%u) failed: %s\n",
                calibrated_qd_, strerror(-ret));
        return false;
    }

    anchor_ring_ = ring;
    anchor_fd_   = ring->ring_fd;
    available_   = true;

    fprintf(stderr,
            "IoUringPool: anchor ring initialised (QD=%u, ring_fd=%d)\n",
            calibrated_qd_, anchor_fd_);
    return true;
}

void IoUringPool::watch_child(int pidfd, uint64_t user_data) {
    if (!available_) return;
    auto* ring = static_cast<io_uring*>(anchor_ring_);

    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        // SQ is full — flush to make room (shouldn't happen for small N)
        io_uring_submit(ring);
        sqe = io_uring_get_sqe(ring);
        if (!sqe)
            throw std::runtime_error("IoUringPool::watch_child: SQ full");
    }

    io_uring_prep_poll_add(sqe, pidfd, POLLIN);
    sqe->user_data = user_data;
    io_uring_submit(ring);
}

std::vector<uint64_t> IoUringPool::wait_any() {
    if (!available_) return {};
    auto* ring = static_cast<io_uring*>(anchor_ring_);

    // Block until at least one CQE arrives
    struct io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(ring, &cqe);
    if (ret < 0)
        throw std::runtime_error(
            std::string("IoUringPool::wait_any: ") + strerror(-ret));

    // Drain all available CQEs in one batch
    std::vector<uint64_t> results;
    unsigned head = 0;
    struct io_uring_cqe* c = nullptr;
    io_uring_for_each_cqe(ring, head, c) {
        results.push_back(c->user_data);
    }
    io_uring_cq_advance(ring, static_cast<unsigned>(results.size()));
    return results;
}

void* IoUringPool::create_child_ring_struct() {
    auto* ring = new io_uring{};

    struct io_uring_params params{};
    if (anchor_fd_ >= 0) {
        params.flags |= IORING_SETUP_ATTACH_WQ;
        params.wq_fd  = static_cast<unsigned>(anchor_fd_);
    }

    int ret = io_uring_queue_init_params(calibrated_qd_, ring, &params);
    if (ret < 0) {
        fprintf(stderr,
                "IoUringPool::create_child_ring_struct: ATTACH_WQ failed: %s"
                " — retrying as plain ring\n",
                strerror(-ret));
        params = {};
        ret = io_uring_queue_init_params(calibrated_qd_, ring, &params);
        if (ret < 0) {
            delete ring;
            fprintf(stderr,
                    "IoUringPool::create_child_ring_struct: plain ring also failed: %s\n",
                    strerror(-ret));
            return nullptr;
        }
    }
    return ring;
}

void IoUringPool::free_ring(void* ring_struct) {
    if (!ring_struct) return;
    auto* ring = static_cast<io_uring*>(ring_struct);
    io_uring_queue_exit(ring);
    delete ring;
}

// ---- stub (no io_uring) -------------------------------------------------
#else

bool IoUringPool::init(const std::string& /*output_dir*/) { return false; }
void IoUringPool::watch_child(int /*pidfd*/, uint64_t /*user_data*/) {}
std::vector<uint64_t> IoUringPool::wait_any() { return {}; }
void* IoUringPool::create_child_ring_struct() { return nullptr; }
void  IoUringPool::free_ring(void* /*ring*/) {}

#endif  // TPCH_ENABLE_ASYNC_IO

// ---- always-available accessors -----------------------------------------
bool     IoUringPool::available()   { return available_; }
int      IoUringPool::anchor_fd()   { return anchor_fd_; }
uint32_t IoUringPool::queue_depth() { return calibrated_qd_; }

}  // namespace tpch
