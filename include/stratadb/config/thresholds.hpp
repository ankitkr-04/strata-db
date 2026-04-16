#pragma once
#include <cstddef>
namespace stratadb::config {
namespace defaults {
inline constexpr std::size_t THRESHOLDS_MAX_BYTE = (64ULL * 1024 * 1024);            // 64MB
inline constexpr std::size_t THRESHOLDS_FLUSH_TRIGGER_BYTES = (48ULL * 1024 * 1024); // 48MB
inline constexpr std::size_t THRESHOLDS_STALL_TRIGGER_BYTES = (60ULL * 1024 * 1024); // 60MB
} // namespace defaults

struct MemTableThresholds {
    std::size_t max_byte{defaults::THRESHOLDS_MAX_BYTE};
    std::size_t flush_trigger_bytes{defaults::THRESHOLDS_FLUSH_TRIGGER_BYTES};
    std::size_t stall_trigger_bytes{defaults::THRESHOLDS_STALL_TRIGGER_BYTES};
};
} // namespace stratadb::config