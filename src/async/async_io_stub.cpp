// Stub implementation for AsyncIOContext when async I/O is disabled

#include "tpch/async_io.hpp"
#include "tpch/shared_async_io.hpp"

#ifndef TPCH_ENABLE_ASYNC_IO

#include <unistd.h>
#include <fcntl.h>
#include <stdexcept>

namespace tpch {

// AsyncIOContext stub implementations
void AsyncIOContext::submit_write(int fd, const void* buf, std::size_t count, off_t offset) {
    // Fallback to synchronous write
    ssize_t result = pwrite(fd, buf, count, offset);
    (void)result;  // Ignore result for stub
}

void AsyncIOContext::queue_write(int fd, const void* buf, std::size_t count, off_t offset, std::uint64_t user_data) {
    // Fallback to immediate synchronous write
    (void)user_data;
    ssize_t result = pwrite(fd, buf, count, offset);
    (void)result;  // Ignore result for stub
}

// SharedAsyncIOContext stub implementations
SharedAsyncIOContext::SharedAsyncIOContext(size_t queue_depth) {
    (void)queue_depth;
}

SharedAsyncIOContext::SharedAsyncIOContext(const AsyncIOConfig& config) {
    (void)config;
}

int SharedAsyncIOContext::register_file(const std::string& path) {
    // Open file synchronously
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::runtime_error("Failed to open file: " + path);
    }
    return fd;
}

void SharedAsyncIOContext::queue_write(int file_handle, const void* buf, size_t count) {
    // Synchronous write
    ssize_t result = write(file_handle, buf, count);
    (void)result;
}

int SharedAsyncIOContext::submit_all() {
    return 0;  // Nothing to submit in stub
}

int SharedAsyncIOContext::wait_any(int min_complete) {
    (void)min_complete;
    return 0;  // Nothing to wait for in stub
}

void SharedAsyncIOContext::flush() {
    // Nothing to flush in stub
}

int SharedAsyncIOContext::pending_count() const {
    return 0;  // No pending operations in stub
}

void SharedAsyncIOContext::close_file(int file_handle) {
    close(file_handle);
}

void SharedAsyncIOContext::close_all() {
    // Nothing to close in stub
}

off_t SharedAsyncIOContext::get_offset(int file_handle) const {
    (void)file_handle;
    return 0;
}

}  // namespace tpch

#endif  // !TPCH_ENABLE_ASYNC_IO
