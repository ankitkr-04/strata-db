#pragma once

#include "stratadb/utils/cache.hpp"
#include "stratadb/wal/types.hpp"

#include <atomic>

namespace stratadb::wal {

// VyukovMpscQueue — Intrusive Lock-Free MPSC Queue.
//
// Producer (push): Wait-Free — completes in bounded steps via a single XCHG.
// Consumer (pop) : Lock-Free — handles the brief "producer stalled mid-link"
//                  race by returning nullptr and letting the caller retry.
//
// The stub_ node acts as a permanent dummy head so the queue is never truly
// empty from a structural standpoint; the logical empty check compares
// head == tail.
//
// Cache-line layout:
//   stub_ occupies one line (modified only during init and pop).
//   head_ occupies its own line (modified only by the consumer).
//   tail_ occupies its own line (the hottest variable — modified by every push).
//   consumer_sleeping_ occupies its own line (written by both sides, rarely).
class VyukovMpscQueue {
  public:
    VyukovMpscQueue() noexcept {
        stub_.pool_managed = false; // stub is stack-allocated; do not recycle
        stub_.next.store(nullptr, std::memory_order_relaxed);
        head_.store(&stub_, std::memory_order_relaxed);
        tail_.store(&stub_, std::memory_order_relaxed);
    }

    ~VyukovMpscQueue() = default;

    VyukovMpscQueue(const VyukovMpscQueue&) = delete;
    auto operator=(const VyukovMpscQueue&) -> VyukovMpscQueue& = delete;
    VyukovMpscQueue(VyukovMpscQueue&&) = delete;
    auto operator=(VyukovMpscQueue&&) -> VyukovMpscQueue& = delete;

    // Wait-free: a single atomic XCHG unconditionally swaps the tail.
    // The link step (prev->next = node) is a plain release store and is
    // always visible to the consumer before the next push on the same thread.
    void push(MpscNode* node) noexcept {
        node->next.store(nullptr, std::memory_order_relaxed);

        MpscNode* prev = tail_.exchange(node, std::memory_order_acq_rel);
        prev->next.store(node, std::memory_order_release);

        // Wake a sleeping flusher.  The double-checked load avoids a costly
        // seq_cst store on the common (non-sleeping) path.
        if (consumer_sleeping_.load(std::memory_order_seq_cst)) {
            consumer_sleeping_.store(false, std::memory_order_relaxed);
            consumer_sleeping_.notify_one();
        }
    }

    // Returns {nullptr, nullptr} when the queue is empty OR when a producer
    // has swapped the tail but not yet linked prev->next (nanosecond window).
    // The caller must spin / back off and retry on a null result.
    [[nodiscard]] auto pop() noexcept -> PopResultData {
        MpscNode* head = head_.load(std::memory_order_relaxed);
        MpscNode* next = head->next.load(std::memory_order_acquire);

        if (head == tail_.load(std::memory_order_relaxed)) {
            // Logically empty — but 'next != nullptr' means a producer is
            // mid-push (stalled between XCHG and the link store).  Fall
            // through: we'll return nullptr and the caller will retry.
            if (next == nullptr) {
                return {};
            }
        }

        if (next != nullptr) {
            // node_to_free = old dummy head (safe to recycle after this return).
            // payload_node = next (the new dummy, which carries the payload).
            head_.store(next, std::memory_order_relaxed);
            return {.payload_node = next, .node_to_free = head};
        }

        return {}; // producer stalled; caller retries
    }

    // Called by the flusher when pop() returns empty.  Sleeps on the futex
    // until a producer calls push() or force_wakeup() is invoked.
    void wait_for_work(std::atomic<bool>& stop_requested) noexcept {
        consumer_sleeping_.store(true, std::memory_order_seq_cst);

        // Recheck after setting the flag to close the race where a producer
        // pushed just before we wrote consumer_sleeping_ = true.
        if (head_.load(std::memory_order_relaxed)->next.load(std::memory_order_acquire) != nullptr
            || stop_requested.load(std::memory_order_acquire)) {
            consumer_sleeping_.store(false, std::memory_order_relaxed);
            return;
        }

        consumer_sleeping_.wait(true, std::memory_order_relaxed);
        consumer_sleeping_.store(false, std::memory_order_relaxed);
    }

    void force_wakeup() noexcept {
        if (consumer_sleeping_.load(std::memory_order_seq_cst)) {
            consumer_sleeping_.store(false, std::memory_order_relaxed);
            consumer_sleeping_.notify_one();
        }
    }

  private:
    alignas(utils::CACHE_LINE_SIZE) MpscNode stub_{};
    alignas(utils::CACHE_LINE_SIZE) std::atomic<MpscNode*> head_{};
    alignas(utils::CACHE_LINE_SIZE) std::atomic<MpscNode*> tail_{};
    alignas(utils::CACHE_LINE_SIZE) std::atomic<bool> consumer_sleeping_{false};
};

} // namespace stratadb::wal