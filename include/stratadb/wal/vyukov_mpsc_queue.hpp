#pragma once

#include "stratadb/utils/hardware.hpp"
#include "stratadb/wal/wal_concept.hpp"

#include <atomic>
#include <cstddef>
#include <type_traits>

namespace stratadb::wal {

// Vyukov's Intrusive Lock-Free MPSC Queue.
// - Producers: Wait-Free (Guaranteed to complete in bounded steps via XCHG).
// - Consumer: Lock-Free (Handles the ProducerStalled quirk).
class VyukovMpscQueue {
  public:
    VyukovMpscQueue() noexcept {
        stub_.next.store(nullptr, std::memory_order_relaxed);
        head_.store(&stub_, std::memory_order_relaxed);
        tail_.store(&stub_, std::memory_order_relaxed);
    }

    ~VyukovMpscQueue() = default;

    // Non-copyable, non-movable (it's a static hardware bridge)
    VyukovMpscQueue(const VyukovMpscQueue&) = delete;
    auto operator=(const VyukovMpscQueue&) -> VyukovMpscQueue& = delete;
    VyukovMpscQueue(VyukovMpscQueue&&) = delete;
    auto operator=(VyukovMpscQueue&&) -> VyukovMpscQueue& = delete;

    // --- PRODUCER HOT PATH (Executed by N Database Writers) ---
    void push(MpscNode* node) noexcept {
        // 1. Prepare the node
        node->next.store(nullptr, std::memory_order_relaxed);

        // 2. The Wait-Free XCHG (Swaps the tail unconditionally)
        MpscNode* prev = tail_.exchange(node, std::memory_order_acq_rel);

        // 3. Link the list
        prev->next.store(node, std::memory_order_release);

        // 4. Wake the flusher if it went to sleep to save power
        if (consumer_sleeping_.load(std::memory_order_relaxed)) {
            consumer_sleeping_.store(false, std::memory_order_relaxed);
            consumer_sleeping_.notify_one(); // Linux futex wake
        }
    }

    // --- CONSUMER HOT PATH (Executed by 1 Flusher Thread) ---
    [[nodiscard]] auto pop() noexcept -> MpscNode* {
        MpscNode* head = head_.load(std::memory_order_relaxed);
        MpscNode* next = head->next.load(std::memory_order_acquire);

        // Case 1: Queue is genuinely empty
        if (head == tail_.load(std::memory_order_relaxed)) {
            if (next == nullptr) {
                return nullptr; // Empty
            }
            // Edge Case: ProducerStalled!
            // A producer swapped the tail but hasn't updated `prev->next` yet.
            // We fall through and spin briefly.
        }

        // Case 2: Normal Pop
        if (next != nullptr) {
            // The popped node BECOMES the new dummy head.
            head_.store(next, std::memory_order_relaxed);

            // We return the OLD head (which was the dummy, but now holds the
            // payload of the previously popped item, or is empty if it's the stub).
            // NOTE: The caller extracts the data from `next`, but mathematically
            // we return `head` so it can be recycled.
            // For simplicity in StrataDB, we actually return `next` and transfer
            // ownership, keeping the memory alive.
            return next;
        }

        // Case 3: Producer is stalled mid-instruction. Spin to yield CPU.
        // This is a nanosecond-scale race condition.
        return nullptr;
    }

    // --- CONSUMER POWER MANAGEMENT ---
    // Called by the flusher when `pop()` returns nullptr.
    void wait_for_work() noexcept {
        consumer_sleeping_.store(true, std::memory_order_relaxed);

        // Double check to prevent race condition where a producer pushed
        // right before we set the sleeping flag.
        if (head_.load(std::memory_order_relaxed)->next.load(std::memory_order_acquire) != nullptr) {
            consumer_sleeping_.store(false, std::memory_order_relaxed);
            return;
        }

        // Sleep on the futex natively via C++20
        consumer_sleeping_.wait(true, std::memory_order_acquire);
    }

  private:
    // ALIGNMENT STRICTNESS:
    // Head and Tail MUST be on separate cache lines to prevent False Sharing.

    alignas(utils::CACHE_LINE_SIZE) MpscNode stub_{};
    alignas(utils::CACHE_LINE_SIZE) std::atomic<MpscNode*> head_{};

    // The most contended variable in the entire database
    alignas(utils::CACHE_LINE_SIZE) std::atomic<MpscNode*> tail_{};

    // The futex wake variable
    alignas(utils::CACHE_LINE_SIZE) std::atomic<bool> consumer_sleeping_{false};
};

} // namespace stratadb::wal