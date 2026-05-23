#pragma once

#include "stratadb/memory/block_pool.hpp"
#include "stratadb/utils/cache.hpp"
#include "stratadb/utils/limits.hpp"
#include "stratadb/utils/thread.hpp"
#include "stratadb/wal/concepts.hpp"
#include "stratadb/wal/types.hpp"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <span>

namespace stratadb::wal {

// WalPipeline<Layout, Queue>
//
// Ties together a physical block format (Layout) and a lock-free handoff queue
// (Queue) into the hot write path.  All dispatch is zero-cost: the template
// parameters are resolved at compile time; there are no virtual calls.
//
// Memory layout per BlockPool block:
//
//   [0, LAYOUT_OFFSET)         — FlushResult (queue node + I/O descriptor).
//   [LAYOUT_OFFSET, block_size) — Layout instance (GammaBlock / DeltaBlock).
//
// Each thread uses its dense index (utils::get_dense_thread_index()) to index
// into active_blocks_[] so there is zero contention on the staging path.
template <WALBlockLayout Layout, ConcurrencyQueue Queue>
class WalPipeline {
  public:
    // Offset in bytes from the start of a BlockPool block to the Layout object.
    // 4 KiB — one hardware sector; keeps the Layout 4 KiB-aligned inside the
    // block without needing a separate aligned allocation.
    static constexpr std::size_t LAYOUT_OFFSET = 4096;

    static_assert(sizeof(FlushResult) <= LAYOUT_OFFSET,
                  "FlushResult overlaps the Layout region; increase LAYOUT_OFFSET");

    WalPipeline(memory::BlockPool& pool, std::atomic<std::uint64_t>& lsn_gen)
        : pool_(pool)
        , lsn_generator_(lsn_gen) {
        // Runtime check: pool blocks must be large enough to hold both the
        // FlushResult header region and the Layout object.
        assert(LAYOUT_OFFSET + sizeof(Layout) <= pool_.block_size()
               && "BlockPool block_size too small for this Layout; "
                  "increase BlockPoolConfig::block_size_bytes");

        for (auto& s : per_thread_sentinels_) {
            s.pool_managed = false; // sentinels are static; do not recycle
        }
    }

    // ── Hot write path ───────────────────────────────────────────────────────
    // Called by any writer thread.  Zero virtual dispatch; inlined by the
    // compiler when stage_write is instantiated through WalManager::write_batch.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    auto stage_write(std::span<const std::byte> key, std::span<const std::byte> value) noexcept -> bool {
        const std::size_t tid = utils::get_dense_thread_index() % utils::MAX_SUPPORTED_THREADS;
        auto& active = active_blocks_[tid];

        if (active.empty()) {
            allocate_new_block(tid);
        }

        Layout* block = layout_at(active);
        if (!block->append(key, value)) {
            seal_and_dispatch(tid);
            allocate_new_block(tid);
            block = layout_at(active_blocks_[tid]);
            // A freshly allocated block must always have room for at least one
            // record; if this fires the record itself exceeds block capacity.
            return block->append(key, value);
        }

        return true;
    }

    [[nodiscard]] auto pop_ready_block() noexcept -> PopResultData {
        return handoff_queue_.pop();
    }

    void wait_for_work(std::atomic<bool>& stop_requested) noexcept {
        handoff_queue_.wait_for_work(stop_requested);
    }

    // Force all thread-local partially-filled blocks through the queue.
    // Iterates all MAX_SUPPORTED_THREADS slots; safe only when all writers
    // have finished staging (i.e. called from a flush fence or destructor).
    void flush_pipeline() noexcept {
        for (std::size_t i = 0; i < utils::MAX_SUPPORTED_THREADS; ++i) {
            if (!active_blocks_[i].empty()) {
                seal_and_dispatch(i);
            }
        }
        // Push a sentinel to wake a sleeping flusher without carrying data.
        const std::size_t tid = utils::get_dense_thread_index() % utils::MAX_SUPPORTED_THREADS;
        handoff_queue_.push(&per_thread_sentinels_[tid]);
    }

    // Called from WalManager destructor only.  Seals remaining blocks and
    // wakes the flusher but does NOT push an extra sentinel node so that
    // the flusher's empty-check remains reliable at shutdown.
    void shutdown() noexcept {
        for (std::size_t i = 0; i < utils::MAX_SUPPORTED_THREADS; ++i) {
            if (!active_blocks_[i].empty()) {
                seal_and_dispatch(i);
            }
        }
        handoff_queue_.force_wakeup();
    }

  private:
    Queue handoff_queue_;
    memory::BlockPool& pool_;
    std::atomic<std::uint64_t>& lsn_generator_;

    // Sentinels live here — static storage, never pool-recycled.
    alignas(utils::CACHE_LINE_SIZE) FlushResult per_thread_sentinels_[utils::MAX_SUPPORTED_THREADS]{};

    // One span per thread tracking the currently active block (empty = none).
    alignas(utils::CACHE_LINE_SIZE) std::span<std::byte> active_blocks_[utils::MAX_SUPPORTED_THREADS]{};

    [[nodiscard]] auto layout_at(std::span<std::byte> block) noexcept -> Layout* {
        return reinterpret_cast<Layout*>(block.data() + LAYOUT_OFFSET);
    }

    void allocate_new_block(std::size_t tid) noexcept {
        active_blocks_[tid] = pool_.acquire_block();
        auto* block = new (layout_at(active_blocks_[tid])) Layout();
        const std::uint64_t lsn = lsn_generator_.fetch_add(1, std::memory_order_relaxed);
        block->init(lsn);
    }

    void seal_and_dispatch(std::size_t tid) noexcept {
        auto memory = active_blocks_[tid];
        auto* block = layout_at(memory);

        FlushResult result = block->finalize(block->header.sequence_number);

        // Placement-new the FlushResult at offset 0 of the same block so
        // the queue node and the I/O descriptor share a single allocation.
        auto* queue_node = new (memory.data()) FlushResult{
            .memory_to_write = result.memory_to_write,
            .block_internal_offset = result.block_internal_offset,
            .max_lsn = result.max_lsn,
        };
        // pool_managed defaults to true in MpscNode — the flusher will
        // return this block to the pool after the I/O completes.

        handoff_queue_.push(queue_node);
        active_blocks_[tid] = {};
    }
};

} // namespace stratadb::wal