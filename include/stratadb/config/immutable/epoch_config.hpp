#pragma once

#include <cstddef>
#include <cstdint>

namespace stratadb::config {

struct EpochConfig {

    static constexpr std::size_t DEFAULT_RECLAIM_INTERVAL = 64UZ; // must be power of 2
    static constexpr std::size_t DEFAULT_RETIRE_LIST_THRESHOLD = 10'000UZ;
    static constexpr std::uint32_t DEFAULT_BACKOFF_MAX_US = 1'000U;

    // Trigger epoch advance + reclaim every N retire() calls.
    // Resolver enforces: must be a power of two.
    std::size_t reclaim_interval{DEFAULT_RECLAIM_INTERVAL};

    // Yield if a single thread accumulates this many unreclaimed pointers.
    // Above this threshold EpochManager applies exponential backoff sleep.
    std::size_t retire_list_threshold{DEFAULT_RETIRE_LIST_THRESHOLD};

    // Cap on the exponential backoff sleep (microseconds).
    std::uint32_t backoff_max_us{DEFAULT_BACKOFF_MAX_US};
};

} // namespace stratadb::config