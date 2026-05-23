#pragma once

#include <cstddef>
#include <new>

namespace stratadb::utils {

// Destructive interference size: minimum padding to prevent false sharing.
// Use this to align/pad hot fields that are written by different threads.
#if defined(__cpp_lib_hardware_interference_size) && (__cpp_lib_hardware_interference_size >= 201603L)
inline constexpr std::size_t CACHE_LINE_SIZE = std::hardware_destructive_interference_size;
// Constructive interference size: maximum size for data accessed together.
// Use this to group fields that are always read together by the same thread.
inline constexpr std::size_t CACHE_LINE_CONSTRUCT_SIZE = std::hardware_constructive_interference_size;
#else
inline constexpr std::size_t CACHE_LINE_SIZE = 64UZ;
inline constexpr std::size_t CACHE_LINE_CONSTRUCT_SIZE = 64UZ;
#endif

} // namespace stratadb::utils