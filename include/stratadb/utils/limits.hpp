#pragma once

#include <cstddef>

namespace stratadb::utils {

// Hard upper bound on concurrently registered threads.
// Sizes epoch bitmask arrays and thread-local slot tables.
// Must be a multiple of 64 (one word per epoch bitmask lane).
inline constexpr std::size_t MAX_SUPPORTED_THREADS = 256UZ;

// Hard upper bound on simultaneously open DB instances per process.
// Sizes the O(1) thread-local instance dispatch table.
inline constexpr std::size_t MAX_DB_INSTANCES = 64UZ;

// Hard upper bound on skip-list tower height.
// Used as the compile-time array bound for Splice::Level so the hot-path
// stack frame stays fixed-size. SkipListConfig::max_height is validated
// against this at resolver time.
inline constexpr std::size_t MAX_SKIPLIST_HEIGHT = 32UZ;

} // namespace stratadb::utils