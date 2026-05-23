#pragma once

#include <cstdint>

namespace stratadb::config {

enum class NumaPolicy : std::uint8_t {
    UMA,         // Ignore NUMA topology; rely on OS scheduler.
    Interleaved, // Stripe allocation across all NUMA nodes (predictable latency).
    StrictLocal, // Pin threads and allocate strictly on their local NUMA node.
};

} // namespace stratadb::config