#pragma once

#include "stratadb/utils/bytes.hpp"

#include <cstddef>

namespace stratadb::config {

using namespace stratadb::utils::bytes::literals;

struct MemTableConfig {

    static constexpr std::size_t DEFAULT_MAX_SIZE_BYTES = 64_MiB;
    static constexpr std::size_t DEFAULT_FLUSH_TRIGGER_BYTES = 48_MiB;
    static constexpr std::size_t DEFAULT_STALL_TRIGGER_BYTES = 60_MiB;

    std::size_t max_size_bytes{DEFAULT_MAX_SIZE_BYTES};
    std::size_t flush_trigger_bytes{DEFAULT_FLUSH_TRIGGER_BYTES};
    std::size_t stall_trigger_bytes{DEFAULT_STALL_TRIGGER_BYTES};
};

} // namespace stratadb::config