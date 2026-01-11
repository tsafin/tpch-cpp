#ifndef TPCH_ARENA_ALLOCATOR_HPP
#define TPCH_ARENA_ALLOCATOR_HPP

#include <array>
#include <cstddef>
#include <memory>
#include <new>

namespace tpch {

/**
 * Stack-based arena allocator for small temporary allocations.
 *
 * This arena provides a fast, cache-friendly memory pool that lives on the stack.
 * It's designed for temporary allocations that can be bulk-deallocated together,
 * which is common in batch processing operations.
 *
 * Benefits:
 * - Zero malloc/free overhead for allocations within arena size
 * - Excellent cache locality (contiguous memory)
 * - No fragmentation
 * - Automatic cleanup on scope exit
 *
 * Typical usage: Temporary buffers in hot path conversions
 */
template<size_t Size>
class StackArena {
public:
    static constexpr size_t size = Size;

    StackArena() : ptr_(buffer_.data()) {}

    /**
     * Allocate n bytes with the specified alignment.
     *
     * @param n Number of bytes to allocate
     * @param alignment Alignment requirement (default: std::max_align_t)
     * @return Pointer to allocated memory, or nullptr if arena is full
     */
    void* allocate(size_t n, size_t alignment = alignof(std::max_align_t)) {
        // Align pointer
        auto current = reinterpret_cast<uintptr_t>(ptr_);
        auto aligned = (current + alignment - 1) & ~(alignment - 1);
        auto aligned_ptr = reinterpret_cast<char*>(aligned);

        // Check if we have enough space
        if (aligned_ptr + n > buffer_.data() + Size) {
            return nullptr;  // Arena exhausted, caller should fallback to heap
        }

        ptr_ = aligned_ptr + n;
        return aligned_ptr;
    }

    /**
     * Reset the arena, effectively deallocating all previous allocations.
     * This is very fast (single pointer write).
     */
    void reset() {
        ptr_ = buffer_.data();
    }

    /**
     * Get the number of bytes currently allocated.
     */
    size_t used() const {
        return ptr_ - buffer_.data();
    }

    /**
     * Get the number of bytes available.
     */
    size_t available() const {
        return Size - used();
    }

private:
    std::array<char, Size> buffer_;
    char* ptr_;
};

/**
 * STL-compatible allocator that uses a StackArena.
 *
 * This allows using std::vector and other containers with arena allocation.
 * When the arena is full, it automatically falls back to heap allocation.
 *
 * Example:
 *   StackArena<1024*1024> arena;  // 1MB
 *   std::vector<int64_t, ArenaAllocator<int64_t, 1024*1024>> vec(
 *       ArenaAllocator<int64_t, 1024*1024>(arena));
 */
template<typename T, size_t ArenaSize>
class ArenaAllocator {
public:
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    explicit ArenaAllocator(StackArena<ArenaSize>& arena)
        : arena_(&arena), heap_allocated_(false) {}

    // Copy constructor for rebind
    template<typename U>
    ArenaAllocator(const ArenaAllocator<U, ArenaSize>& other) noexcept
        : arena_(other.arena_), heap_allocated_(false) {}

    T* allocate(size_t n) {
        void* ptr = arena_->allocate(n * sizeof(T), alignof(T));
        if (ptr) {
            return static_cast<T*>(ptr);
        }

        // Arena full, fallback to heap
        heap_allocated_ = true;
        return static_cast<T*>(::operator new(n * sizeof(T)));
    }

    void deallocate(T* ptr, size_t n) {
        // Arena allocations are bulk-deallocated on reset()
        // Only free heap allocations
        if (heap_allocated_) {
            ::operator delete(ptr);
        }
        // Note: This is simplified. A production implementation would need
        // to track which pointers came from arena vs heap.
    }

    // Rebind for container support
    template<typename U>
    struct rebind {
        using other = ArenaAllocator<U, ArenaSize>;
    };

    // For compatibility
    StackArena<ArenaSize>* arena_;
    bool heap_allocated_;
};

// Equality comparison (required for allocator)
template<typename T, typename U, size_t Size>
bool operator==(const ArenaAllocator<T, Size>& a, const ArenaAllocator<U, Size>& b) {
    return a.arena_ == b.arena_;
}

template<typename T, typename U, size_t Size>
bool operator!=(const ArenaAllocator<T, Size>& a, const ArenaAllocator<U, Size>& b) {
    return !(a == b);
}

}  // namespace tpch

#endif  // TPCH_ARENA_ALLOCATOR_HPP
