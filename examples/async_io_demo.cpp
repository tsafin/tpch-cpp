#include <iostream>
#include <chrono>
#include <memory>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <sys/stat.h>
#include <cerrno>

#ifdef TPCH_ENABLE_ASYNC_IO
#include "tpch/async_io.hpp"
#endif

using Clock = std::chrono::high_resolution_clock;

/**
 * Synchronous write benchmark - baseline comparison
 */
void benchmark_sync_write(const std::string& filename, size_t num_writes, size_t write_size) {
    std::cout << "\n=== Synchronous Write Benchmark ===" << std::endl;
    std::cout << "File: " << filename << std::endl;
    std::cout << "Number of writes: " << num_writes << std::endl;
    std::cout << "Write size: " << write_size << " bytes" << std::endl;

    // Allocate write buffer
    auto buffer = std::make_unique<char[]>(write_size);
    std::memset(buffer.get(), 'A', write_size);

    // Open file for writing
    int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        std::cerr << "Failed to open file: " << strerror(errno) << std::endl;
        return;
    }

    // Benchmark synchronous writes
    auto start = Clock::now();

    for (size_t i = 0; i < num_writes; ++i) {
        ssize_t written = write(fd, buffer.get(), write_size);
        if (written < 0) {
            std::cerr << "Write failed: " << strerror(errno) << std::endl;
            close(fd);
            return;
        }
        if (static_cast<size_t>(written) != write_size) {
            std::cerr << "Partial write: expected " << write_size << ", got " << written << std::endl;
            close(fd);
            return;
        }
    }

    auto end = Clock::now();
    close(fd);

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    size_t total_bytes = num_writes * write_size;
    double throughput = (duration > 0) ? (static_cast<double>(total_bytes) / (1024.0 * 1024.0) / (duration / 1000.0)) : 0.0;

    std::cout << "Time: " << duration << " ms" << std::endl;
    std::cout << "Total written: " << (total_bytes / (1024 * 1024)) << " MB" << std::endl;
    std::cout << "Throughput: " << throughput << " MB/s" << std::endl;
}

#ifdef TPCH_ENABLE_ASYNC_IO

/**
 * Asynchronous write benchmark using io_uring
 */
void benchmark_async_write(const std::string& filename, size_t num_writes, size_t write_size) {
    std::cout << "\n=== Asynchronous Write Benchmark (io_uring) ===" << std::endl;
    std::cout << "File: " << filename << std::endl;
    std::cout << "Number of writes: " << num_writes << std::endl;
    std::cout << "Write size: " << write_size << " bytes" << std::endl;

    try {
        // Allocate write buffers
        std::vector<std::unique_ptr<char[]>> buffers;
        for (size_t i = 0; i < num_writes; ++i) {
            auto buffer = std::make_unique<char[]>(write_size);
            std::memset(buffer.get(), 'B', write_size);
            buffers.push_back(std::move(buffer));
        }

        // Open file for writing
        int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            std::cerr << "Failed to open file: " << strerror(errno) << std::endl;
            return;
        }

        // Create async I/O context
        tpch::AsyncIOContext aio(256);

        // Benchmark asynchronous writes
        auto start = Clock::now();

        // Submit all writes
        off_t offset = 0;
        for (size_t i = 0; i < num_writes; ++i) {
            aio.submit_write(fd, buffers[i].get(), write_size, offset);
            offset += write_size;

            // Flush periodically to avoid queue overflow
            if ((i + 1) % 64 == 0) {
                aio.wait_completions(32);
            }
        }

        // Wait for all pending operations
        aio.flush();

        auto end = Clock::now();
        close(fd);

        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        size_t total_bytes = num_writes * write_size;
        double throughput = (duration > 0) ? (static_cast<double>(total_bytes) / (1024.0 * 1024.0) / (duration / 1000.0)) : 0.0;

        std::cout << "Time: " << duration << " ms" << std::endl;
        std::cout << "Total written: " << (total_bytes / (1024 * 1024)) << " MB" << std::endl;
        std::cout << "Throughput: " << throughput << " MB/s" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "Async I/O error: " << e.what() << std::endl;
    }
}

/**
 * Run comparison between sync and async
 */
void run_comparison(size_t num_writes, size_t write_size) {
    std::cout << "\n========================================" << std::endl;
    std::cout << "Async I/O Performance Comparison" << std::endl;
    std::cout << "========================================" << std::endl;

    std::string sync_file = "/tmp/async_demo_sync.dat";
    std::string async_file = "/tmp/async_demo_async.dat";

    benchmark_sync_write(sync_file, num_writes, write_size);
    benchmark_async_write(async_file, num_writes, write_size);

    // Verify files were created
    struct stat sb;
    if (stat(sync_file.c_str(), &sb) == 0) {
        std::cout << "\nSync file size: " << sb.st_size << " bytes" << std::endl;
    }
    if (stat(async_file.c_str(), &sb) == 0) {
        std::cout << "Async file size: " << sb.st_size << " bytes" << std::endl;
    }

    std::cout << "\nNote: Async I/O benefits increase with larger queue depths and concurrent operations" << std::endl;
}

#endif  // TPCH_ENABLE_ASYNC_IO

int main() {
#ifdef TPCH_ENABLE_ASYNC_IO
    std::cout << "=== Async I/O Demo (io_uring enabled) ===" << std::endl;

    // Test parameters
    const size_t num_writes = 256;      // Number of write operations
    const size_t write_size = 64 * 1024; // 64KB per write

    run_comparison(num_writes, write_size);

#else
    std::cout << "Async I/O demo not available (TPCH_ENABLE_ASYNC_IO not defined)" << std::endl;
    std::cout << "To enable, rebuild with: cmake -B build -DTPCH_ENABLE_ASYNC_IO=ON" << std::endl;
    std::cout << "You also need liburing installed: sudo apt-get install liburing-dev" << std::endl;
#endif

    return 0;
}
