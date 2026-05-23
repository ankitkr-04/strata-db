#pragma once

#include <cstdint>

namespace stratadb::config {

struct SkipListConfig {

    static constexpr std::uint8_t DEFAULT_MAX_HEIGHT = 12U;
    static constexpr std::uint8_t DEFAULT_BRANCHING_FACTOR = 4U; // must be power of 2

    // Maximum tower height. Controls the memory-vs-search-time trade-off.
    // Resolver enforces: [1, utils::MAX_SKIPLIST_HEIGHT].
    std::uint8_t max_height{DEFAULT_MAX_HEIGHT};

    // Probability denominator for level promotion: 1 / branching_factor per level.
    // Resolver enforces: must be a power of two.
    std::uint8_t branching_factor{DEFAULT_BRANCHING_FACTOR};
};

} // namespace stratadb::config