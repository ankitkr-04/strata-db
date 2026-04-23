#pragma once

#include <cstddef>

namespace stratadb::config {

struct MemTableConfig {
    static constexpr std::size_t KiB = 1024ULL;
    static constexpr std::size_t MiB = KiB * 1024ULL;

    static constexpr std::size_t DEFAULT_MAX_SIZE_BYTES = 64ULL * MiB;
    static constexpr std::size_t DEFAULT_FLUSH_TRIGGER_BYTES = 48ULL * MiB;
    static constexpr std::size_t DEFAULT_STALL_TRIGGER_BYTES = 60ULL * MiB;

    std::size_t max_size_bytes{DEFAULT_MAX_SIZE_BYTES};
    std::size_t flush_trigger_bytes{DEFAULT_FLUSH_TRIGGER_BYTES};
    std::size_t stall_trigger_bytes{DEFAULT_STALL_TRIGGER_BYTES};
};

} // namespace stratadb::config
