#include "stratadb/memory/epoch_manager.hpp"

#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <thread>

namespace stratadb::memory {

void EpochManager::register_thread() {
    // Each thread must register exactly once
    assert(thread_index_ == INVALID_THREAD);
    if (thread_index_ != INVALID_THREAD) {
        std::terminate();
    }

    for (std::size_t i = 0; i < defaults::MAX_THREADS; i++) {
        bool expected = false;

        // Try to atomically claim a free slot (false → true)
        // acq_rel ensures proper visibility of ownership across threads
        if (thread_states_[i].in_use.compare_exchange_strong(expected,
                                                             true,
                                                             std::memory_order_acq_rel,
                                                             std::memory_order_relaxed)) {

            // Store slot index in thread-local storage
            thread_index_ = i;
            return;
        }
    }

    // No free slot available → hard failure
    throw std::runtime_error("Exceeded maximum number of threads for EpochManager");
}

void EpochManager::unregister_thread() {
    assert(thread_index_ != INVALID_THREAD);

    auto& state = thread_states_[thread_index_];

    // Ensure thread is not visible as active
    state.state.store(UINT64_MAX, std::memory_order_release);

    reclaim();

    state.in_use.store(false, std::memory_order_release);

    thread_index_ = INVALID_THREAD;
}

void EpochManager::enter() noexcept {
    assert(thread_index_ != INVALID_THREAD);
    uint64_t e = global_epoch_.load(std::memory_order_acquire);

    thread_states_[thread_index_].state.store(e, std::memory_order_release);
}

void EpochManager::leave() noexcept {
    assert(thread_index_ != INVALID_THREAD);
    thread_states_[thread_index_].state.store(UINT64_MAX, std::memory_order_release);
}

void EpochManager::advance_epoch() noexcept {
    // Advance global epoch (logical time)
    // acq_rel ensures ordering with readers/writers
    global_epoch_.fetch_add(1, std::memory_order_acq_rel);
}

void EpochManager::retire_node(void* ptr, void (*deleter)(void*)) noexcept {
    assert(thread_index_ != INVALID_THREAD);
    auto& state = thread_states_[thread_index_];

    // Tag node with current epoch
    // Meaning: may still be visible to threads in ≤ this epoch
    uint64_t retire_epoch = global_epoch_.load(std::memory_order_acquire);

    // Add to thread-local retire list (no synchronization needed)
    state.retire_list_.push_back(RetireNode{.ptr = ptr, .deleter = deleter, .retire_epoch = retire_epoch});

    if ((state.retire_list_.size() & defaults::RECLAIM_MASK) == 0) [[unlikely]] {
        // Periodically attempt to reclaim (tuning parameter)
        reclaim();

        if (state.retire_list_.size() > defaults::RETIRE_LIST_THRESHOLD) {
            std::this_thread::yield(); // back off if retire list grows too large (tuning parameter)
        }
    }
}

void EpochManager::reclaim() noexcept {
    assert(thread_index_ != INVALID_THREAD);

    // Start with current global epoch
    uint64_t min_epoch = global_epoch_.load(std::memory_order_acquire);

    // Find minimum epoch among all ACTIVE threads
    // This represents the oldest possible reader
    for (const ThreadState& state : thread_states_) {

        if (!state.in_use.load(std::memory_order_acquire))
            continue;

        uint64_t e = state.state.load(std::memory_order_acquire);
        if (e != UINT64_MAX)
            min_epoch = std::min(min_epoch, e);
    }

    // Only reclaim from THIS thread's retire list (ownership invariant)
    auto& retire_list = thread_states_[thread_index_].retire_list_;

    // Safe to delete nodes strictly older than min_epoch
    // (< is critical; <= would risk use-after-free)
    std::erase_if(retire_list, [&](auto& node) {
        if (node.retire_epoch < min_epoch) {
            node.deleter(node.ptr);
            return true;
        }
        return false;
    });
}

} // namespace stratadb::memory