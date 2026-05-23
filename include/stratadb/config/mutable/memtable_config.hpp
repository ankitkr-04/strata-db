#pragma once

#include "stratadb/utils/bytes.hpp"

#include <cstddef>

namespace stratadb::config {

struct MemTableConfig {
    static constexpr std::size_t DEFAULT_FLUSH_TRIGGER_BYTES = 48UZ * stratadb::utils::bytes::MiB;
    static constexpr std::size_t DEFAULT_STALL_TRIGGER_BYTES = 60UZ * stratadb::utils::bytes::MiB;
    static constexpr std::size_t DEFAULT_MAX_SIZE_BYTES = 64UZ * stratadb::utils::bytes::MiB;

    static_assert(DEFAULT_FLUSH_TRIGGER_BYTES < DEFAULT_STALL_TRIGGER_BYTES,
                  "flush trigger must be strictly less than stall trigger");
    static_assert(DEFAULT_STALL_TRIGGER_BYTES < DEFAULT_MAX_SIZE_BYTES,
                  "stall trigger must be strictly less than max size");

    // Fields ordered by threshold value so the invariant reads top-to-bottom.
    std::size_t flush_trigger_bytes{DEFAULT_FLUSH_TRIGGER_BYTES};
    std::size_t stall_trigger_bytes{DEFAULT_STALL_TRIGGER_BYTES};
    std::size_t max_size_bytes{DEFAULT_MAX_SIZE_BYTES};
};

} // namespace stratadb::config