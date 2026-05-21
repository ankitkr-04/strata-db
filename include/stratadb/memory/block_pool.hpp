#pragma once

#include "stratadb/utils/bytes.hpp"
#include "stratadb/utils/hardware.hpp"

#include <atomic>
#include <cassert> // Required for defensive boundary contracts
#include <cstddef>
#include <cstdint>
#include <span>

namespace stratadb::memory {

// Forward declare the test peer wrapper
namespace test {
struct BlockPoolTestPeer;
}

class BlockPool {
  public:
    static constexpr size_t BLOCK_SIZE = 32 * stratadb::utils::bytes::KiB;
    static constexpr size_t CAPACITY = 16384;
    static constexpr size_t INDEX_MASK = CAPACITY - 1;
    static constexpr size_t PAYLOAD_ARENA_ALIGNMENT = 4 * stratadb::utils::bytes::KiB;

    BlockPool();
    ~BlockPool();
    BlockPool(const BlockPool&) = delete;
    auto operator=(const BlockPool&) -> BlockPool& = delete;
    BlockPool(BlockPool&&) = delete;
    auto operator=(BlockPool&&) -> BlockPool& = delete;

    [[nodiscard]] inline auto acquire_block() noexcept -> std::span<std::byte> {
        uint64_t current_head = head_.load(std::memory_order_acquire);

        while (true) {
            uint64_t current_tail = tail_.load(std::memory_order_acquire);

            // FIX 1: Signed distance computation to survive 2^64 integer wrap-around safely
            if (static_cast<int64_t>(current_tail - current_head) <= 0) {
                tail_.wait(current_tail, std::memory_order_acquire);
                current_head = head_.load(std::memory_order_acquire);
                continue;
            }

            if (head_.compare_exchange_weak(current_head,
                                            current_head + 1,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
                const uint16_t block_id = routing_ring_[current_head & INDEX_MASK].load(std::memory_order_relaxed);
                return std::span<std::byte>{payload_arena_ + (static_cast<size_t>(block_id) * BLOCK_SIZE), BLOCK_SIZE};
            }
        }
    }

    inline void release_block(std::span<std::byte> returned_block) noexcept {
        // FIX 2: Prevent hostile or out-of-bounds pointers from silently corrupting the lock-free routing ring
        assert(returned_block.data() >= payload_arena_);
        assert(returned_block.data() < payload_arena_ + (CAPACITY * BLOCK_SIZE));

        const auto byte_offset = static_cast<size_t>(returned_block.data() - payload_arena_);
        assert(byte_offset % BLOCK_SIZE == 0);

        const auto block_id = static_cast<uint16_t>(byte_offset / BLOCK_SIZE);

        const uint64_t current_tail = tail_.load(std::memory_order_relaxed);

        routing_ring_[current_tail & INDEX_MASK].store(block_id, std::memory_order_release);
        tail_.store(current_tail + 1, std::memory_order_release);
        tail_.notify_one();
    }

  private:
    // Architectural testing bridge: strictly grants access to the isolated testing struct
    friend struct test::BlockPoolTestPeer;

    std::byte* payload_arena_{nullptr};
    std::atomic<uint16_t> routing_ring_[CAPACITY];

    alignas(stratadb::utils::CACHE_LINE_SIZE) std::atomic_uint64_t head_{0};
    alignas(stratadb::utils::CACHE_LINE_SIZE) std::atomic_uint64_t tail_{0};
};
} // namespace stratadb::memory