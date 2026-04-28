#pragma once

#include "stratadb/config/io_config.hpp"
#include "stratadb/utils/bytes.hpp"

#include <chrono>
#include <cstddef>

namespace stratadb::config {
using namespace stratadb::utils::bytes::literals;
using namespace std::chrono_literals;

struct WalConfig {

    // Hardware AWUPF(Atomic Write Unit Power Fail) is typically 4KiB, so we align WAL blocks to that for maximum
    // durability.
    static constexpr std::size_t DEFAULT_WAL_BLOCK_SIZE = 4_KiB;

    // How much data WAL can stange in memory before forcing a hard disk flush
    static constexpr std::size_t DEFAULT_MAX_STAGING_BYTES = 128_MiB;

    IoStrategy io_strategy{IoStrategy::AutoDetect};

    std::size_t wal_block_size_bytes{DEFAULT_WAL_BLOCK_SIZE};
    std::size_t max_staging_bytes{DEFAULT_MAX_STAGING_BYTES};

    // 0 means Auto-Detect via OS Probing. > 0 forces a specific queue depth.
    std::size_t target_queue_depth{0};

    // Used by AdaptiveBuffered to detect SSD GC stalls
    std::chrono::microseconds target_flush_latency{50us};
};
} // namespace stratadb::config