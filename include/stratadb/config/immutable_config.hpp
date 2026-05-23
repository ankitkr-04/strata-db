#pragma once

#include "stratadb/config/immutable/memory_config.hpp"
#include "stratadb/config/immutable/wal_config.hpp"

#include <cstddef>

namespace stratadb::config {

struct ImmutableConfig {
    // SSTable block encoding / decoding granularity.
    // Independent of MemoryConfig::block_alignment_bytes (allocator alignment).
    static constexpr std::size_t DEFAULT_BLOCK_SIZE_BYTES = 4096UZ;

    std::size_t block_size_bytes{DEFAULT_BLOCK_SIZE_BYTES};

    MemoryConfig memory{};
    WalConfig wal{};
};

} // namespace stratadb::config