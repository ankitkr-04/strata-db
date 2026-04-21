#pragma once

#include <cstddef>

namespace stratadb::config {

struct MemTableConfig {
    static constexpr std::size_t DEFAULT_MAX_BYTES = 64ULL * 1024 * 1024;
    static constexpr std::size_t DEFAULT_FLUSH_TRIGGER_BYTES = 48ULL * 1024 * 1024;
    static constexpr std::size_t DEFAULT_STALL_TRIGGER_BYTES = 60ULL * 1024 * 1024;

    std::size_t max_bytes{DEFAULT_MAX_BYTES};
    std::size_t flush_trigger_bytes{DEFAULT_FLUSH_TRIGGER_BYTES};
    std::size_t stall_trigger_bytes{DEFAULT_STALL_TRIGGER_BYTES};
};

} // namespace stratadb::config
