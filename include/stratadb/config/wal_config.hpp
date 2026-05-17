#pragma once

#include "stratadb/config/io_config.hpp"
#include "stratadb/utils/bytes.hpp"

#include <chrono>
#include <cstddef>

namespace stratadb::config {
using namespace stratadb::utils::bytes::literals;
using namespace std::chrono_literals;

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
    // OPT-IN: If set, StrataDB will attempt to seize this specific physical core,
    // pin the Flusher to it, and deploy the Zero-Latency SPSC Mailbox architecture.
    // If nullopt, safely defaults to Vyukov Intrusive MPSC.
    std::optional<std::uint32_t> dedicated_flusher_core_id{std::nullopt};

    // Attempt to elevate the Flusher to Real-Time priority (requires CAP_SYS_NICE).
    bool request_realtime_priority{true};

    // Safety check: wait for fsync/fdatasync before acknowledging the WriteBatch
    bool sync_on_commit{true};
};

} // namespace stratadb::config