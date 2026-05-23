#pragma once

#include "stratadb/config/io_config.hpp"
#include "stratadb/utils/bytes.hpp"

#include <chrono>
#include <cstddef>

namespace stratadb::config {
using namespace std::chrono_literals;

enum class SpscMode : std::uint8_t {
    Disabled,      // Standard Vyukov MPSC queue. No core pinning.
    AutoDiscover,  // StrataDB finds the highest isolated core automatically.
    ManualOverride // User explicitly defines the core in `core_id`.
};

struct SpscConfig {
    SpscMode mode{SpscMode::Disabled};
    std::optional<std::uint32_t> core_id{std::nullopt};
};

struct WalConfig {
    /// How much data WAL can stage in the BlockPool before blocking writers.
    static constexpr std::size_t DEFAULT_MAX_STAGING_BYTES = 128 * stratadb::utils::bytes::MiB;

    /// I/O policy specifically applied to the WAL's file descriptor.
    /// (SSTables will have their own IoConfig in their own builder).
    IOConfig io_config{};

    std::size_t max_staging_bytes{DEFAULT_MAX_STAGING_BYTES};

    /// Micro-batching timeout. If the Flusher thread receives a partial block
    /// and no other threads push data within this window, it flushes to disk.
    static constexpr std::chrono::microseconds DEFAULT_TARGET_FLUSH_LATENCY{50};
    std::chrono::microseconds target_flush_latency{DEFAULT_TARGET_FLUSH_LATENCY};

    SpscConfig spsc_config{};

    /// Attempt to elevate the Flusher to Real-Time priority (requires CAP_SYS_NICE).
    bool request_realtime_priority{true};

    /// Wait for fsync/fdatasync before acknowledging the WriteBatch.
    bool sync_on_commit{true};

    /// Number of pre-allocated, fdatasync'd files to maintain in the Ready state.
    /// If our fixed Ring Buffer has 5 slots, this cannot exceed 3 (1 Active, 1 Creating, 3 Ready).
    /// Defaulting to 2 absorbs virtually any realistic NVMe burst.
    std::uint8_t ready_file_count{2};

    /// Capacity threshold (0.0 to 1.0) of the Active file that triggers the background
    /// thread to wake up and replenish the Ready pool.
    float precreate_trigger_ratio{0.75f};
};

} // namespace stratadb::config