#pragma once

#include "stratadb/config/io_config.hpp"
#include "stratadb/utils/bytes.hpp"

#include <chrono>
#include <cstddef>

namespace stratadb::config {
using namespace stratadb::utils::bytes::literals;
using namespace std::chrono_literals;

enum class SpscMode : std::uint8_t {
    Disabled,      // Standard Vyukov MPSC queue. No core pinning.
    AutoDiscover,  // StrataDB finds the highest isolated core automatically.
    ManualOverride // User explicitly defines the core in `manual_core_id`.
};
struct WalConfig {

    // How much data WAL can stage in the BlockPool before blocking writers.
    static constexpr std::size_t DEFAULT_MAX_STAGING_BYTES = 128_MiB;

    // --- MECHANISM ---
    // The I/O policy specifically applied to the WAL's file descriptor.
    // (SSTables will have their own IoConfig in their own builder).
    IOConfig io_config{};

    // --- POLICY ---
    std::size_t max_staging_bytes{DEFAULT_MAX_STAGING_BYTES};

    // The micro-batching timeout. If the Flusher thread receives a partial block
    // and no other threads push data within this window, it flushes to disk.
    std::chrono::microseconds target_flush_latency{50us};

    // --- CONCURRENCY & SCHEDULING (The Fast Path) ---
    SpscMode spsc_mode{SpscMode::Disabled};

    // ONLY used if spsc_mode == SpscMode::ManualOverride
    std::optional<std::uint32_t> manual_core_id{std::nullopt};

    // Attempt to elevate the Flusher to Real-Time priority (requires CAP_SYS_NICE).
    bool request_realtime_priority{true};

    // Safety check: wait for fsync/fdatasync before acknowledging the WriteBatch
    bool sync_on_commit{true};
};

} // namespace stratadb::config