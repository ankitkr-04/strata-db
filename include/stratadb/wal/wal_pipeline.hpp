#pragma once

#include "stratadb/memory/block_pool.hpp"
#include "stratadb/wal/spsc_mailbox_queue.hpp"
#include "stratadb/wal/wal_concept.hpp"

#include <cstddef>
#include <span>

namespace stratadb::wal {

template <WALBlockLayout Layout, ConcurrencyQueue Queue>
class WalPipeline {
  public:
    WalPipeline(memory::BlockPool& pool, std::atomic<uint64_t>& lsn_gen)
        : pool_(pool)
        , lsn_generator_(lsn_gen) {}

    // The Hot Path: Inlined, zero virtual dispatch.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto stage_write(std::span<const std::byte> key, std::span<const std::byte> value) noexcept -> bool {
        const std::size_t tid = get_dense_thread_index() % utils::MAX_SUPPORTED_THREADS;
        auto& active_span = active_blocks_[tid];

        if (active_span.empty()) {
            allocate_new_block(tid);
        }

        Layout* block = get_layout(active_span);

        if (!block->append(key, value)) {
            seal_and_dispatch(tid);
            allocate_new_block(tid);
            block = get_layout(active_span);
            return block->append(key, value); // This must succeed since we just allocated a fresh block
        }

        return true;
    }

    [[nodiscard]] auto pop_ready_block() noexcept -> FlushResult* {
        return static_cast<FlushResult*>(handoff_queue_.pop());
    }

    void wait_for_work() noexcept {
        handoff_queue_.wait_for_work();
    }

    void flush_pipeline() noexcept {
        // 1. Forcefully seal and dispatch ALL active thread-local blocks 
        // (This simulates the Micro-Batching Timeout for our tests)
        for (std::size_t i = 0; i < utils::MAX_SUPPORTED_THREADS; ++i) {
            if (!active_blocks_[i].empty()) {
                seal_and_dispatch(i);
            }
        }

        // 2. Push an empty dummy node to unconditionally wake the Flusher thread
        static FlushResult dummy_node{};
        dummy_node.memory_to_write = {};
        dummy_node.max_lsn = 0; // 0 means ignore
        handoff_queue_.push(&dummy_node);
    }

  private:
    Queue handoff_queue_;
    memory::BlockPool& pool_;
    std::atomic<uint64_t>& lsn_generator_;

    // Thread-Local raw memory tracking without standard `thread_local` overhead
    alignas(utils::CACHE_LINE_SIZE) std::span<std::byte> active_blocks_[utils::MAX_SUPPORTED_THREADS]{};

    // 64-byte offset ensures the Layout starts on a clean cache line
    static constexpr size_t LAYOUT_OFFSET = utils::CACHE_LINE_SIZE;

    inline auto get_layout(std::span<std::byte> memory) noexcept -> Layout* {
        return reinterpret_cast<Layout*>(memory.data() + LAYOUT_OFFSET);
    }

    void allocate_new_block(std::size_t tid) noexcept {
        active_blocks_[tid] = pool_.acquire_block();

        // Placement-new the physical block inside the pre-allocated chunk
        auto* block = new (get_layout(active_blocks_[tid])) Layout();

        // Assign the next Logical Sequence Number
        uint64_t next_lsn = lsn_generator_.fetch_add(1, std::memory_order_relaxed);
        block->init(next_lsn);
    }

    void seal_and_dispatch(std::size_t tid) noexcept {
        auto memory = active_blocks_[tid];
        auto* block = get_layout(memory);

        // Finalize the block, which computes CRCs/XXH3, stamps the final LSN, and returns the exact memory to flush.
        FlushResult result = block->finalize(0);

        // Copy the stack result into the front of 16 KiB aligned memory, which is what the I/O engine expects.
        auto* queue_node = new (memory.data()) FlushResult{
            .memory_to_write = result.memory_to_write,
            .block_internal_offset = result.block_internal_offset,
            .max_lsn = result.max_lsn,
        };

        // pass the chunk via 8-byte base class pointer
        handoff_queue_.push(queue_node);

        // clear the pointer so next write will trigger new block allocation
        active_blocks_[tid] = {};
    }
};
}; // namespace stratadb::wal
// namespace stratadb::wal