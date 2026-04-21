#pragma once

#include "stratadb/config/memory_config.hpp"

#include <cstddef>

namespace stratadb::config {

struct ImmutableConfig {
    static constexpr std::size_t DEFAULT_BLOCK_SIZE = 4096;

    std::size_t block_size{DEFAULT_BLOCK_SIZE};

    MemoryConfig memory_config{};
};

} // namespace stratadb::config
