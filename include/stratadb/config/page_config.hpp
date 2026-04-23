#pragma once

#include <cstdint>

namespace stratadb::config {

enum class PageStrategy : std::uint8_t {
    Standard4K,           // Safe, works everywhere.
    Huge2M_Opportunistic, // Try MAP_HUGE_2MB, fallback to 4K without crashing.
    Huge2M_Strict,        // Require MAP_HUGE_2MB, abort if unavailable.
    Huge1G_Opportunistic, // Try MAP_HUGE_1GB, fallback to 2MB then 4K without crashing.
    Huge1G_Strict         // Bleeding-edge bare-metal only.
};

enum class NumaPolicy : std::uint8_t {
    UMA,         // Ignore NUMA, rely on OS scheduler.
    Interleaved, // Striped allocation across nodes (predictable latency).
    StrictLocal  // Pin threads & allocate strictly local to the node.
};

} // namespace stratadb::config
