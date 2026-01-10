#include "tpch/shared_async_io.hpp"
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <sstream>

namespace tpch {

SharedAsyncIOContext::SharedAsyncIOContext(size_t queue_depth)
    : async_ctx_(std::make_shared<AsyncIOContext>(queue_depth)) {
}

SharedAsyncIOContext::SharedAsyncIOContext(const AsyncIOConfig& config)
    : async_ctx_(std::make_shared<AsyncIOContext>(config)) {
}

int SharedAsyncIOContext::register_file(const std::string& path) {
    // Open file for writing
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        std::ostringstream oss;
        oss << "Failed to open file: " << path;
        throw std::runtime_error(oss.str());
    }

    // Store file state
    int file_handle = next_file_handle_++;
    FileState state;
    state.fd = fd;
    state.path = path;
    state.offset = 0;
    files_[file_handle] = state;

    return file_handle;
}

void SharedAsyncIOContext::queue_write(int file_handle, const void* buf, size_t count) {
    auto it = files_.find(file_handle);
    if (it == files_.end()) {
        throw std::runtime_error("Invalid file handle");
    }

    FileState& state = it->second;

    // Queue write with user_data encoding the file_handle
    // This allows tracking which file each completion belongs to
    uint64_t user_data = static_cast<uint64_t>(file_handle);
    async_ctx_->queue_write(state.fd, buf, count, state.offset, user_data);

    // Advance offset for this file
    state.offset += count;
}

int SharedAsyncIOContext::submit_all() {
    return async_ctx_->submit_queued();
}

int SharedAsyncIOContext::wait_any(int min_complete) {
    std::vector<uint64_t> completed_ids;
    return async_ctx_->wait_completions(min_complete, &completed_ids);
}

void SharedAsyncIOContext::flush() {
    async_ctx_->flush();
}

int SharedAsyncIOContext::pending_count() const {
    return async_ctx_->pending_count();
}

void SharedAsyncIOContext::close_file(int file_handle) {
    auto it = files_.find(file_handle);
    if (it != files_.end()) {
        if (it->second.fd != -1) {
            close(it->second.fd);
        }
        files_.erase(it);
    }
}

void SharedAsyncIOContext::close_all() {
    // Flush any pending I/O
    flush();

    // Close all files
    for (auto& entry : files_) {
        if (entry.second.fd != -1) {
            close(entry.second.fd);
            entry.second.fd = -1;
        }
    }
    files_.clear();
}

off_t SharedAsyncIOContext::get_offset(int file_handle) const {
    auto it = files_.find(file_handle);
    if (it == files_.end()) {
        throw std::runtime_error("Invalid file handle");
    }
    return it->second.offset;
}

}  // namespace tpch
