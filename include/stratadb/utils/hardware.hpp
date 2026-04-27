#pragma once

#include <cstddef>
#include <cstdint>
#include <new>

namespace stratadb::utils {
#if defined(__cpp_lib_hardware_interference_size) && (__cpp_lib_hardware_interference_size >= 201603L)
inline constexpr std::size_t CACHE_LINE_SIZE = std::hardware_destructive_interference_size;
inline constexpr std::size_t CACHE_CONSTRUCT_SIZE = std::hardware_constructive_interference_size;
#else
inline constexpr std::size_t CACHE_LINE_SIZE = 64UZ;
inline constexpr std::size_t CACHE_CONSTRUCT_SIZE = 64UZ;
#endif

// A safe upper bound for thread-local arrays and epoch masks, allowing massive
// server-grade concurrency without dynamic allocation. Must be a multiple of 64.
inline constexpr std::size_t MAX_SUPPORTED_THREADS = 256;

inline constexpr std::size_t DEFAULT_ATOMIC_WRITE_BOUNDARY = 4096;

// Probes the OS for the disk's physical sector size (AWUPF).
[[nodiscard]] auto probe_atomic_write_boundary(int fd,
                                               std::size_t fallback_size = DEFAULT_ATOMIC_WRITE_BOUNDARY) noexcept
    -> std::size_t;

// Probes the OS for the memory management page size (4K vs 16K vs 64K).
[[nodiscard]] auto system_page_size() noexcept -> std::size_t;

// Probes the hardware for available logical cores (respecting OS constraints).
[[nodiscard]] auto logical_core_count() noexcept -> std::uint32_t;

} // namespace stratadb::utils
