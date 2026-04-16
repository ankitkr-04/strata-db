#pragma once
#include <chrono>
#include <cstdint>
using namespace std::chrono_literals;
namespace stratadb::config {

enum class WalSyncPolicy : std::uint8_t { Immediate, Batch, Async };

namespace defaults {
inline constexpr int WAL_MAX_BATCH_SIZE = 1024;
inline constexpr auto WAL_MAX_INTERVAL = 10ms;
} // namespace defaults

struct WalConfig {
    WalSyncPolicy policy{WalSyncPolicy::Immediate};

    int max_batch_size{defaults::WAL_MAX_BATCH_SIZE};
    std::chrono::milliseconds max_interval_ms{defaults::WAL_MAX_INTERVAL};
};

} // namespace stratadb::config
