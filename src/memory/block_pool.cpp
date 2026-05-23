#include "stratadb/memory/block_pool.hpp"

#include <cstdlib>
#include <numeric>
#include <stdexcept>

namespace stratadb::memory {

BlockPool::BlockPool(const config::BlockPoolConfig& cfg)
    : block_size_(cfg.block_size_bytes)
    , capacity_(cfg.capacity)
    , index_mask_(cfg.capacity - 1) {

    void* ptr = std::aligned_alloc(cfg.payload_alignment_bytes, block_size_ * capacity_);

    if (ptr == nullptr) {
        throw std::bad_alloc();
    }

    payload_arena_ = static_cast<std::byte*>(ptr);

    std::iota(routing_ring_, routing_ring_ + capacity_, 0);

    // head_ tracks consumer progress. Starts at 0.
    head_.store(0, std::memory_order_relaxed);

    // tail_ tracks available item slot bounds. Starts at capacity_
    // because all slots are pre-filled with blocks 0 to capacity_-1.
    tail_.store(capacity_, std::memory_order_relaxed);
}

BlockPool::~BlockPool() noexcept {
    if (payload_arena_) {
        std::free(payload_arena_);
    }
}

} // namespace stratadb::memory