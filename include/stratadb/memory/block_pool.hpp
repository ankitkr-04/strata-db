#pragma once

#include "stratadb/config/immutable/block_pool_config.hpp"
#include "stratadb/utils/cache.hpp"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <span>

namespace stratadb::memory {

// Forward-declare the test bridge so it stays in its own header.
namespace test {
struct BlockPoolTestPeer;
}

class BlockPool {
  public:
    // Maximum block identity that fits in the routing ring slot type.
    // Resolver enforces capacity <= MAX_CAPACITY.
    static constexpr std::size_t MAX_CAPACITY = 65535UZ;

    // ConfigResolver must have validated cfg before construction:
    //   - capacity is a power of two and <= MAX_CAPACITY
    //   - block_size_bytes is a power of two
    //   - payload_alignment_bytes is a power of two and > 0
    explicit BlockPool(const config::BlockPoolConfig& cfg);
    ~BlockPool() noexcept;

    BlockPool(const BlockPool&) = delete;
    auto operator=(const BlockPool&) -> BlockPool& = delete;
    BlockPool(BlockPool&&) = delete;
    auto operator=(BlockPool&&) -> BlockPool& = delete;

    [[nodiscard]] inline auto acquire_block() noexcept -> std::span<std::byte> {
        std::uint64_t current_head = head_.load(std::memory_order_acquire);

        while (true) {
            const std::uint64_t current_tail = tail_.load(std::memory_order_acquire);

            // Signed distance survives 2^64 wrap-around.
            if (static_cast<std::int64_t>(current_tail - current_head) <= 0) {
                tail_.wait(current_tail, std::memory_order_acquire);
                current_head = head_.load(std::memory_order_acquire);
                continue;
            }

            if (head_.compare_exchange_weak(current_head,
                                            current_head + 1,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
                const std::uint16_t block_id =
                    routing_ring_[current_head & index_mask_].load(std::memory_order_relaxed);
                return {payload_arena_ + (static_cast<std::size_t>(block_id) * block_size_), block_size_};
            }
        }
    }

    inline void release_block(std::span<std::byte> returned_block) noexcept {
        // Guard against hostile or out-of-bounds pointers corrupting the ring.
        assert(returned_block.data() >= payload_arena_);
        assert(returned_block.data() < payload_arena_ + (capacity_ * block_size_));
        const auto byte_offset = static_cast<std::size_t>(returned_block.data() - payload_arena_);
        assert(byte_offset % block_size_ == 0);

        const auto block_id = static_cast<std::uint16_t>(byte_offset / block_size_);
        const std::uint64_t current_tail = tail_.load(std::memory_order_relaxed);

        routing_ring_[current_tail & index_mask_].store(block_id, std::memory_order_release);
        tail_.store(current_tail + 1, std::memory_order_release);
        tail_.notify_one();
    }

    [[nodiscard]] auto block_size() const noexcept -> std::size_t {
        return block_size_;
    }
    [[nodiscard]] auto capacity() const noexcept -> std::size_t {
        return capacity_;
    }

  private:
    friend struct test::BlockPoolTestPeer;

    std::size_t block_size_;
    std::size_t capacity_;
    std::size_t index_mask_; // capacity_ - 1; valid because capacity_ is a power of two

    std::byte* payload_arena_{nullptr};
    std::atomic<std::uint16_t>* routing_ring_{nullptr};

    alignas(stratadb::utils::CACHE_LINE_SIZE) std::atomic_uint64_t head_{0};
    alignas(stratadb::utils::CACHE_LINE_SIZE) std::atomic_uint64_t tail_{0};
};

} // namespace stratadb::memory