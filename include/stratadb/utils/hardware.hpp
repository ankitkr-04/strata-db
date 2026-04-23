#pragma once

#include <cstddef>
#include <new>

namespace stratadb::utils {

// 64 bytes is the common L1 cache-line size on x86_64 and ARM64.
inline constexpr std::size_t CACHE_LINE_SIZE = 64;

#if defined(__cpp_lib_hardware_interference_size)
static_assert(std::hardware_destructive_interference_size == CACHE_LINE_SIZE,
			  "CACHE_LINE_SIZE must match std::hardware_destructive_interference_size");
#endif

} // namespace stratadb::utils
