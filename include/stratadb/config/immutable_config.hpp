#pragma once

#include "stratadb/config/memory_config.hpp"

#include <cstddef>

namespace stratadb::config {

struct ImmutableConfig {
    static constexpr std::size_t DEFAULT_BLOCK_SIZE_BYTES = 4096;

    // Owned by future SSTable block encoding/decoding components.
    std::size_t block_size_bytes{DEFAULT_BLOCK_SIZE_BYTES};

    MemoryConfig memory_config{};
};

} // namespace stratadb::config
