#pragma once

#include "stratadb/config/io_config.hpp"
#include "stratadb/utils/bytes.hpp"

#include <chrono>
#include <cstddef>

namespace stratadb::config {
using namespace stratadb::utils::bytes::literals;
using namespace std::chrono_literals;

struct WalConfig {
    // Hardware AWUPF (Atomic Write Unit Power Fail) is typically 4 KiB.
    static constexpr std::size_t DEFAULT_WAL_BLOCK_SIZE = 4_KiB;

    // How much data WAL can stage in the BlockPool before blocking writers.
    static constexpr std::size_t DEFAULT_MAX_STAGING_BYTES = 128_MiB;

    // --- MECHANISM ---
    // The I/O policy specifically applied to the WAL's file descriptor.
    // (SSTables will have their own IoConfig in their own builder).
    IOConfig io_config{};

    // --- POLICY ---
    std::size_t wal_block_size_bytes{DEFAULT_WAL_BLOCK_SIZE};
    std::size_t max_staging_bytes{DEFAULT_MAX_STAGING_BYTES};

    // The micro-batching timeout. If the Flusher thread receives a partial block
    // and no other threads push data within this window, it flushes to disk.
    std::chrono::microseconds target_flush_latency{50us};
};

} // namespace stratadb::config