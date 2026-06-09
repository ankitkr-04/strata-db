#include "stratadb/memory/block_pool.hpp"

#include <cstdlib>
#include <numeric>
#include <stdexcept>

namespace stratadb::memory {

BlockPool::BlockPool(const config::BlockPoolConfig& cfg)
    : block_size_(cfg.block_size_bytes)
    , capacity_(cfg.capacity)
    , index_mask_(cfg.capacity - 1) {

    // Contigously allocate a single large arena for all blocks, to minimize fragmentation and maximize cache locality.
    void* payload_ptr = std::aligned_alloc(cfg.payload_alignment_bytes, block_size_ * capacity_);

    if (payload_ptr == nullptr) {
        throw std::bad_alloc();
    }

    payload_arena_ = static_cast<std::byte*>(payload_ptr);

    // Routing ring: maps each ring position to a block identity (uint16_t).
    // Previously unallocated (routing_ring_ == nullptr) before std::iota —
    // null-pointer UB. Allocate first, then initialise with identity mapping.
    routing_ring_ = new std::atomic<std::uint16_t>[capacity_];
    for (std::size_t i = 0; i < capacity_; ++i) {
        routing_ring_[i].store(static_cast<std::uint16_t>(i), std::memory_order_relaxed);
    }

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
    delete[] routing_ring_; // routing_ring_ is a fixed-size array, so we can safely delete it
}

} // namespace stratadb::memory