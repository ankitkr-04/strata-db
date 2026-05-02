#include "stratadb/memory/block_pool.hpp"

#include <cstdlib>
#include <stdexcept>

namespace stratadb::memory {
BlockPool::BlockPool()
    : routing_ring_{} {
    // 1. Allocate 256MB contiguous payload array, strictly aligned to 4096 (O_DIRECT)
    // Note: std::aligned_alloc requires the total size to be a multiple of the alignment.
    // 16384 * 16384 = 268,435,456, which is cleanly divisible by 4096.
    void* ptr = std::aligned_alloc(PAYLOAD_ARRAY_SIZE, BLOCK_SIZE * CAPACITY);
    if (ptr == nullptr) {
        throw std::bad_alloc();
    }
    payload_arena_ = static_cast<std::byte*>(ptr);

    for (uint16_t i = 0; i < CAPACITY; ++i) {
        routing_ring_[i] = i;
    }

    head_.store(0, std::memory_order_relaxed);
    tail_.store(0, std::memory_order_relaxed);
}

BlockPool::~BlockPool() {
    if (payload_arena_)
        std::free(payload_arena_);
}



} // namespace stratadb::memory