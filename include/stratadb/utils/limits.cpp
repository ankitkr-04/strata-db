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

} // namespace stratadb::utils