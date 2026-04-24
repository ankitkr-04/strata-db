#pragma once

#include <cstddef>
#include <new>

namespace stratadb::utils {

// 64 bytes is the common L1 cache-line size on x86_64 and ARM64.
inline constexpr std::size_t CACHE_LINE_SIZE = 64;

} // namespace stratadb::utils
