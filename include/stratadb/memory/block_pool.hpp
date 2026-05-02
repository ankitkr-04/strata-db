#pragma once

#include "stratadb/utils/bytes.hpp"
#include "stratadb/utils/hardware.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>

namespace stratadb::memory {
using namespace stratadb::utils::bytes::literals;
class BlockPool {

  public:
    static constexpr size_t BLOCK_SIZE = 16_KiB; // 16KB blocks
    static constexpr size_t CAPACITY = 16384;    // 2^14 blocks -> 256MB total
    static constexpr size_t INDEX_MASK = CAPACITY - 1;

    BlockPool();
    ~BlockPool();
    BlockPool(const BlockPool&) = delete;
    auto operator=(const BlockPool&) -> BlockPool& = delete;
    BlockPool(BlockPool&&) = delete;
    auto operator=(BlockPool&&) -> BlockPool& = delete;

    // Multi Consumer
    [[nodiscard]] inline auto acquire_block() noexcept -> std::span<std::byte> {
        uint64_t current_head = head_.load(std::memory_order_acquire);

        while (true) {
            uint64_t current_tail = tail_.load(std::memory_order_acquire);
            if (current_head >= current_tail) {
                // Futex wait,  yield to avoid busy-waiting
                tail_.wait(current_tail, std::memory_order_acquire);
                current_head = head_.load(std::memory_order_acquire);
                continue;
            }

            if (head_.compare_exchange_weak(current_head,
                                            current_head + 1,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
                const uint16_t block_id = routing_ring_[current_head & INDEX_MASK];
                return std::span<std::byte>{payload_arena_ + (static_cast<size_t>(block_id) * BLOCK_SIZE), BLOCK_SIZE};
            }
        }
    }

    // Single Producer
    inline void release_block(std::span<std::byte> returned_block) noexcept {
        const auto byte_offset = static_cast<size_t>(returned_block.data() - payload_arena_);
        const auto block_id = static_cast<uint16_t>(byte_offset / BLOCK_SIZE);

        const uint64_t current_tail = tail_.load(std::memory_order_relaxed);

        routing_ring_[current_tail & INDEX_MASK] = block_id;
        tail_.store(current_tail + 1, std::memory_order_release);
        tail_.notify_one();
    }

  private:
    std::byte* payload_arena_{nullptr};
    uint16_t routing_ring_[CAPACITY];

    alignas(stratadb::utils::CACHE_LINE_SIZE) std::atomic_uint64_t head_{0};
    alignas(stratadb::utils::CACHE_LINE_SIZE) std::atomic_uint64_t tail_{0};
};
} // namespace stratadb::memory