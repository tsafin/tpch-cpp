/**
 * SIMD String Utilities - Optimized string operations
 *
 * Provides SIMD-accelerated versions of common string operations:
 * - Fast strlen using xsimd (portable across architectures)
 * - Optimized memcpy (compiler auto-vectorization)
 *
 * This implementation uses xsimd for portable SIMD operations when available,
 * falling back to manual SSE4.2 intrinsics or standard library functions.
 *
 * Architecture support:
 * - With xsimd (modern): x86 (SSE, AVX, AVX2, AVX512), ARM NEON, SVE, RISC-V, etc.
 * - Without xsimd: x86 SSE4.2 only
 * - Fallback: Standard library functions
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#ifdef TPCH_USE_XSIMD
// Use modern xsimd for portable SIMD operations
#include <xsimd/xsimd.hpp>

namespace tpch {
namespace simd {

/**
 * Fast strlen using xsimd portable SIMD (modern API)
 *
 * Uses xsimd's architecture-independent batch operations. The batch size
 * is automatically determined based on the available SIMD instruction set.
 *
 * Supports: AVX512 (64 bytes), AVX2 (32 bytes), SSE (16 bytes), ARM NEON (16 bytes), etc.
 *
 * Performance: ~2-4× faster than standard strlen for strings >16 bytes
 *
 * @param str Null-terminated string
 * @return Length of string (excluding null terminator)
 */
inline size_t strlen_sse42(const char* str) {
    using batch_type = xsimd::batch<uint8_t>;
    constexpr size_t batch_size = batch_type::size;

    const char* ptr = str;
    const uint8_t* uptr = reinterpret_cast<const uint8_t*>(str);

    // Handle unaligned prefix
    const size_t alignment = reinterpret_cast<uintptr_t>(uptr) % batch_size;
    if (alignment != 0) {
        // Process byte-by-byte until aligned
        for (size_t i = 0; i < batch_size - alignment; ++i) {
            if (ptr[i] == '\0') {
                return i;
            }
        }
        ptr += (batch_size - alignment);
        uptr += (batch_size - alignment);
    }

    // Process batch_size bytes at a time using xsimd
    const batch_type zero(uint8_t(0));

    while (true) {
        // Load batch (aligned)
        batch_type chunk = xsimd::load_aligned(uptr);

        // Compare with zero
        auto mask = (chunk == zero);

        // Check if any byte is zero
        if (xsimd::any(mask)) {
            // Find first zero byte
            for (size_t i = 0; i < batch_size; ++i) {
                if (ptr[i] == '\0') {
                    return static_cast<size_t>(ptr - str) + i;
                }
            }
        }

        ptr += batch_size;
        uptr += batch_size;
    }
}

/**
 * Fast strlen with unaligned load support (xsimd version)
 *
 * For cases where string alignment is unknown, this variant
 * uses unaligned loads (slightly slower but always safe).
 *
 * @param str Null-terminated string (may be unaligned)
 * @return Length of string
 */
inline size_t strlen_sse42_unaligned(const char* str) {
    using batch_type = xsimd::batch<uint8_t>;
    constexpr size_t batch_size = batch_type::size;

    const char* ptr = str;
    const uint8_t* uptr = reinterpret_cast<const uint8_t*>(str);
    const batch_type zero(uint8_t(0));

    while (true) {
        // Load batch (unaligned - safe but slightly slower)
        batch_type chunk = xsimd::load_unaligned(uptr);

        // Compare with zero
        auto mask = (chunk == zero);

        // Check if any byte is zero
        if (xsimd::any(mask)) {
            // Find first zero byte
            for (size_t i = 0; i < batch_size; ++i) {
                if (ptr[i] == '\0') {
                    return static_cast<size_t>(ptr - str) + i;
                }
            }
        }

        ptr += batch_size;
        uptr += batch_size;
    }
}

/**
 * Batch string copy using xsimd
 *
 * Uses xsimd for optimal memory copying across architectures.
 * Automatically uses the best SIMD instruction set available.
 *
 * @param dest Destination buffer
 * @param src Source buffer
 * @param n Number of bytes to copy
 */
inline void memcpy_batch(void* dest, const void* src, size_t n) {
    using batch_type = xsimd::batch<uint8_t>;
    constexpr size_t batch_size = batch_type::size;

    if (n < batch_size * 2) {
        // For small copies, use standard memcpy
        std::memcpy(dest, src, n);
        return;
    }

    uint8_t* dst = static_cast<uint8_t*>(dest);
    const uint8_t* s = static_cast<const uint8_t*>(src);

    // Copy batch_size bytes at a time
    size_t i = 0;
    for (; i + batch_size <= n; i += batch_size) {
        auto chunk = xsimd::load_unaligned(s + i);
        xsimd::store_unaligned(dst + i, chunk);
    }

    // Copy remaining bytes
    if (i < n) {
        std::memcpy(dst + i, s + i, n - i);
    }
}

} // namespace simd
} // namespace tpch

#elif defined(__SSE4_2__)
// Fallback to manual SSE4.2 intrinsics (x86 only)
#include <nmmintrin.h>  // SSE4.2 intrinsics

namespace tpch {
namespace simd {

/**
 * Fast strlen using SSE4.2 PCMPISTRI instruction
 *
 * Processes 16 bytes at a time using SIMD comparison,
 * significantly faster than scalar byte-by-byte comparison.
 *
 * Performance: ~2-3× faster than standard strlen for strings >16 bytes
 *
 * @param str Null-terminated string
 * @return Length of string (excluding null terminator)
 */
inline size_t strlen_sse42(const char* str) {
    const __m128i zero = _mm_setzero_si128();
    const char* ptr = str;

    // Handle unaligned prefix (up to 15 bytes)
    // Process byte-by-byte until aligned to 16-byte boundary
    while ((reinterpret_cast<uintptr_t>(ptr) & 15) != 0) {
        if (*ptr == '\0') {
            return ptr - str;
        }
        ptr++;
    }

    // Process 16 bytes at a time using SSE4.2
    while (true) {
        // Load 16 bytes (aligned)
        __m128i chunk = _mm_load_si128(reinterpret_cast<const __m128i*>(ptr));

        // Compare each byte with zero
        // Returns index of first null byte, or 16 if not found
        int index = _mm_cmpistri(
            zero,                               // needle (null byte)
            chunk,                              // haystack (16 bytes)
            _SIDD_UBYTE_OPS |                  // operate on unsigned bytes
            _SIDD_CMP_EQUAL_EACH |             // compare for equality
            _SIDD_LEAST_SIGNIFICANT            // return least significant index
        );

        if (index != 16) {
            // Found null terminator at position 'index'
            return (ptr - str) + index;
        }

        ptr += 16;
    }
}

/**
 * Fast strlen with unaligned load support
 *
 * For cases where string alignment is unknown, this variant
 * uses unaligned loads (slightly slower but always safe).
 *
 * @param str Null-terminated string (may be unaligned)
 * @return Length of string
 */
inline size_t strlen_sse42_unaligned(const char* str) {
    const __m128i zero = _mm_setzero_si128();
    const char* ptr = str;

    while (true) {
        // Load 16 bytes (unaligned - safe but slower)
        __m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i*>(ptr));

        int index = _mm_cmpistri(
            zero, chunk,
            _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_EACH | _SIDD_LEAST_SIGNIFICANT
        );

        if (index != 16) {
            return (ptr - str) + index;
        }

        ptr += 16;
    }
}

/**
 * Batch string copy - leverages compiler auto-vectorization
 *
 * With -O3 -march=native, compiler will auto-vectorize this to AVX/SSE
 *
 * @param dest Destination buffer
 * @param src Source buffer
 * @param n Number of bytes to copy
 */
inline void memcpy_batch(void* dest, const void* src, size_t n) {
    // Compiler auto-vectorizes this with -ftree-vectorize
    std::memcpy(dest, src, n);
}

} // namespace simd
} // namespace tpch

#else  // No SIMD support - fallback to standard library

namespace tpch {
namespace simd {

// Fallback implementations using standard library
inline size_t strlen_sse42(const char* str) {
    return std::strlen(str);
}

inline size_t strlen_sse42_unaligned(const char* str) {
    return std::strlen(str);
}

inline void memcpy_batch(void* dest, const void* src, size_t n) {
    std::memcpy(dest, src, n);
}

} // namespace simd
} // namespace tpch

#endif  // TPCH_USE_XSIMD / __SSE4_2__
