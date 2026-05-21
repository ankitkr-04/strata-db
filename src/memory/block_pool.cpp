#include "stratadb/memory/block_pool.hpp"

#include <cstdlib>
#include <numeric>
#include <stdexcept>

namespace stratadb::memory {
BlockPool::BlockPool()
    : routing_ring_{} {
    void* ptr = std::aligned_alloc(PAYLOAD_ARENA_ALIGNMENT, BLOCK_SIZE * CAPACITY);
    if (ptr == nullptr) {
        throw std::bad_alloc();
    }
    payload_arena_ = static_cast<std::byte*>(ptr);

    std::iota(routing_ring_, routing_ring_ + CAPACITY, 0);

    // head_ tracks consumer progress. Starts at 0.
    head_.store(0, std::memory_order_relaxed);

    // tail_ tracks available item slot bounds. Starts at CAPACITY
    // because all slots are pre-filled with blocks 0 to CAPACITY-1.
    tail_.store(CAPACITY, std::memory_order_relaxed);
}

BlockPool::~BlockPool() {
    if (payload_arena_)
        std::free(payload_arena_);
}

} // namespace stratadb::memory