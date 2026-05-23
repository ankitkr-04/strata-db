#pragma once

#include "stratadb/utils/bytes.hpp"

#include <chrono>
#include <cstddef>

namespace stratadb::config {

struct WalTuning {
    static constexpr std::size_t DEFAULT_MAX_STAGING_BYTES = 128UZ * stratadb::utils::bytes::MiB;
    static constexpr std::chrono::microseconds DEFAULT_TARGET_FLUSH_LATENCY{50};
    static constexpr float DEFAULT_PRECREATE_TRIGGER_RATIO = 0.75f;

    // Maximum data staged in the BlockPool before writers stall.
    std::size_t max_staging_bytes{DEFAULT_MAX_STAGING_BYTES};

    // Micro-batching window: if the Flusher receives a partial block and no
    // further data arrives within this window, it flushes immediately.
    std::chrono::microseconds target_flush_latency{DEFAULT_TARGET_FLUSH_LATENCY};

    // Active file fill ratio that triggers background replenishment of the Ready pool.
    // Valid range: (0.0, 1.0). Validated at WalManager construction.
    float precreate_trigger_ratio{DEFAULT_PRECREATE_TRIGGER_RATIO};

    // Await fdatasync before acknowledging a WriteBatch to the caller.
    // Disabling trades durability for throughput.
    bool sync_on_commit{true};
};

} // namespace stratadb::config