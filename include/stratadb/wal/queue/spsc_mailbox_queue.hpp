#pragma once

#include "stratadb/utils/cache.hpp"
#include "stratadb/utils/limits.hpp"
#include "stratadb/utils/thread.hpp"
#include "stratadb/wal/types.hpp"

#include <atomic>
#include <cstddef>

namespace stratadb::wal {

// SpscRingBuffer<Capacity>
//
// A single-producer / single-consumer bounded ring buffer for MpscNode pointers.
// Used as a per-thread mailbox: only the owning writer thread pushes into it,
// and only the flusher thread pops from it — no CAS required on either side.
//
// head_ and tail_ are on separate cache lines to eliminate false sharing.
template <std::size_t Capacity = 256>
class alignas(utils::CACHE_LINE_SIZE) SpscRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    static constexpr std::size_t MASK = Capacity - 1;

  public:
    SpscRingBuffer() = default;

    // Producer side — returns false if the ring is full.
    [[nodiscard]] auto try_push(MpscNode* node) noexcept -> bool {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next_tail = (tail + 1) & MASK;
        if (next_tail == head_.load(std::memory_order_acquire)) {
            return false; // full
        }
        buffer_[tail] = node;
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    // Consumer side — returns nullptr if the ring is empty.
    [[nodiscard]] auto try_pop() noexcept -> MpscNode* {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) {
            return nullptr; // empty
        }
        MpscNode* node = buffer_[head];
        head_.store((head + 1) & MASK, std::memory_order_release);
        return node;
    }

  private:
    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::size_t> head_{0};
    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::size_t> tail_{0};
    MpscNode* buffer_[Capacity]{};
};

// SpscMailboxQueue
//
// An array of MAX_SUPPORTED_THREADS independent SPSC ring buffers — one per
// writer thread.  Each writer pushes only into its own mailbox (determined by
// utils::get_dense_thread_index()), so there is zero contention on the push
// path regardless of the number of concurrent writers.
//
// The flusher performs a round-robin sweep across all mailboxes.  This makes
// pop() O(MAX_SUPPORTED_THREADS) in the worst case, but the sweep is a tight
// cache-friendly loop over 256 ring-buffer head pointers.
//
// Power management: the flusher calls cpu_relax() when idle rather than
// sleeping on a futex, making this queue suitable only for SPSC pinned-core
// configurations where busy-polling is acceptable.
class SpscMailboxQueue {
  public:
    SpscMailboxQueue() = default;

    void push(MpscNode* node) noexcept {
        const std::size_t idx = utils::get_dense_thread_index() & THREAD_MASK;
        // Spin-push: if the ring is full the writer waits rather than dropping.
        while (!mailboxes_[idx].try_push(node)) {
            utils::cpu_relax();
        }
    }

    // Flusher sweeps mailboxes starting from where the last pop succeeded
    // (avoids always starving high-index threads).
    [[nodiscard]] auto pop() noexcept -> PopResultData {
        for (std::size_t i = 0; i < utils::MAX_SUPPORTED_THREADS; ++i) {
            const std::size_t idx = (current_sweep_index_ + i) & THREAD_MASK;
            if (MpscNode* node = mailboxes_[idx].try_pop()) {
                current_sweep_index_ = (idx + 1) & THREAD_MASK;
                // SPSC ring: the same node is both the payload and the memory
                // to recycle (no separate dummy-head indirection).
                return {.payload_node = node, .node_to_free = node};
            }
        }
        return {};
    }

    // The SPSC flusher busy-polls; wait_for_work just yields the CPU briefly.
    void wait_for_work(std::atomic<bool>& /*stop_requested*/) noexcept {
        utils::cpu_relax();
    }

    // No-op: the busy-polling flusher never truly sleeps.
    void force_wakeup() noexcept {}

  private:
    static constexpr std::size_t THREAD_MASK =
        utils::MAX_SUPPORTED_THREADS - 1; // for sanity, must be a mask if MAX_SUPPORTED_THREADS is a power of two

    SpscRingBuffer<256> mailboxes_[utils::MAX_SUPPORTED_THREADS];

    // Tracks the next mailbox to inspect; only the flusher thread writes this.
    std::size_t current_sweep_index_{0};

    static_assert((utils::MAX_SUPPORTED_THREADS & THREAD_MASK) == 0, "MAX_SUPPORTED_THREADS must be a power of two");
};

} // namespace stratadb::wal