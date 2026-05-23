#pragma once

#include "stratadb/utils/bytes.hpp"

#include <cstddef>
#include <cstdint>

namespace stratadb::config {

struct BlockPoolConfig {
    static constexpr std::size_t DEFAULT_BLOCK_SIZE_BYTES = 32UZ * stratadb::utils::bytes::KiB;
    static constexpr std::size_t DEFAULT_CAPACITY = 16384UZ; // must be power of 2
    static constexpr std::size_t DEFAULT_PAYLOAD_ALIGNMENT_BYTES = 4UZ * stratadb::utils::bytes::KiB;

    // Size of each write buffer handed to writers and the WAL flusher.
    // Resolver enforces: must be a power of two.
    std::size_t block_size_bytes{DEFAULT_BLOCK_SIZE_BYTES};

    // Number of slots in the lock-free routing ring.
    // Determines the pool's total memory footprint: capacity * block_size_bytes.
    // Resolver enforces: must be a power of two and <= 65535
    //   (block identity is encoded as uint16_t).
    std::size_t capacity{DEFAULT_CAPACITY};

    // Alignment of the payload arena backing the pool.
    // 0 = auto: resolver fills this from the resolved MemoryConfig::block_alignment_bytes.
    std::size_t payload_alignment_bytes{0};
};

} // namespace stratadb::config