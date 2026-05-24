#pragma once

#include "stratadb/config/immutable/io_config.hpp"

#include <cstdint>
#include <optional>

namespace stratadb::config {

enum class SpscMode : std::uint8_t {
    Disabled,      // Standard Vyukov MPSC queue. No core pinning.
    AutoDiscover,  // StrataDB locates the highest isolated core via sysfs.
    ManualOverride // Core is explicitly provided in SpscConfig::core_id.
};

// Groups the SPSC mode and its dependent core_id together so that
// core_id cannot exist without context, and the relationship is self-documenting.
struct SpscConfig {
    SpscMode mode{SpscMode::Disabled};
    // Meaningful only when mode == ManualOverride.
    // Validated against is_core_isolated() at WalManager construction.
    std::optional<std::uint32_t> core_id{std::nullopt};

    // Attempt to elevate the Flusher thread to SCHED_FIFO (requires CAP_SYS_NICE).
    bool request_realtime_priority{true};
};

struct WalConfig {
    // I/O strategy for the WAL file descriptor specifically.
    // (SSTables own a separate IOConfig in their builder.)
    IOConfig io_config{};

    SpscConfig spsc{};

    // Pre-allocated, fdatasync'd files kept in the Ready pool.
    // Total Ring Capacity = 1 (Active) + 1 (Creating) + preallocated_pool_size
    // E.g., A pool size of 2 requires a ring buffer capacity of at least 4.
    std::uint8_t preallocated_pool_size{2};
};

} // namespace stratadb::config