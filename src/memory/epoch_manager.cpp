#include "stratadb/memory/epoch_manager.hpp"

#include <algorithm>
#include <cassert>
#include <bit>
#include <stdexcept>
#include <thread>

namespace stratadb::memory {

void EpochManager::register_thread() {
    // Each thread must register exactly once
    assert(thread_index_ == INVALID_THREAD);
    if (thread_index_ != INVALID_THREAD) {
        std::terminate();
    }

    for (std::size_t word_index = 0; word_index < ACTIVE_THREAD_MASK_WORDS; ++word_index) {
        auto& active_bits = active_thread_masks_[word_index].bits;
        std::uint64_t observed = active_bits.load(std::memory_order_acquire);

        while (observed != ~std::uint64_t{0}) {
            const std::uint64_t available = ~observed;
            const auto bit_index = static_cast<std::size_t>(std::countr_zero(available));
            const std::uint64_t bit = std::uint64_t{1} << bit_index;
            const std::uint64_t desired = observed | bit;

            if (active_bits.compare_exchange_weak(observed,
                                                  desired,
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
                const std::size_t slot = (word_index * 64) + bit_index;
                thread_states_[slot].state.store(INACTIVE_EPOCH, std::memory_order_release);
                thread_index_ = slot;
                return;
            }
        }
    }

    // No free slot available → hard failure
    throw std::runtime_error("Exceeded maximum number of threads for EpochManager");
}

void EpochManager::unregister_thread() {
    assert(thread_index_ != INVALID_THREAD);

    const std::size_t slot = thread_index_;
    auto& state = thread_states_[slot];

    // Ensure thread is not visible as active
    state.state.store(INACTIVE_EPOCH, std::memory_order_release);

    reclaim();

    const std::size_t word_index = slot / 64;
    const std::size_t bit_index = slot % 64;
    const std::uint64_t bit = std::uint64_t{1} << bit_index;
    active_thread_masks_[word_index].bits.fetch_and(~bit, std::memory_order_acq_rel);

    thread_index_ = INVALID_THREAD;
}

void EpochManager::enter() noexcept {
    assert(thread_index_ != INVALID_THREAD);
    std::uint64_t e = global_epoch_.load(std::memory_order_acquire);

    thread_states_[thread_index_].state.store(e, std::memory_order_release);
}

void EpochManager::leave() noexcept {
    assert(thread_index_ != INVALID_THREAD);
    thread_states_[thread_index_].state.store(INACTIVE_EPOCH, std::memory_order_release);
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
    std::uint64_t retire_epoch = global_epoch_.load(std::memory_order_acquire);

    // Add to thread-local retire list (no synchronization needed)
    state.retire_list_.push_back(RetireNode{.ptr = ptr, .deleter = deleter, .retire_epoch = retire_epoch});

    if ((state.retire_list_.size() & RECLAIM_MASK) == 0) [[unlikely]] {
        // Periodically attempt to reclaim (tuning parameter)
        reclaim();

        if (state.retire_list_.size() > RETIRE_LIST_THRESHOLD) {
            std::this_thread::yield(); // back off if retire list grows too large (tuning parameter)
        }
    }
}

void EpochManager::reclaim() noexcept {
    assert(thread_index_ != INVALID_THREAD);

    std::uint64_t min_epoch = global_epoch_.load(std::memory_order_acquire);

    for (std::size_t word_index = 0; word_index < ACTIVE_THREAD_MASK_WORDS; ++word_index) {
        std::uint64_t active_bits = active_thread_masks_[word_index].bits.load(std::memory_order_acquire);

        while (active_bits != 0) {
            const auto bit_index = static_cast<std::size_t>(std::countr_zero(active_bits));
            const std::size_t slot = (word_index * 64) + bit_index;
            const std::uint64_t e = thread_states_[slot].state.load(std::memory_order_acquire);

            if (e != INACTIVE_EPOCH) {
                min_epoch = std::min(min_epoch, e);
            }

            active_bits &= active_bits - 1;
        }
    }

    auto& retire_list = thread_states_[thread_index_].retire_list_;

    // ---- manual compaction (replaces erase_if) ----
    std::size_t write = 0;

    for (std::size_t read = 0; read < retire_list.size(); ++read) {
        auto& node = retire_list[read];

        if (node.retire_epoch < min_epoch) {
            node.deleter(node.ptr); // reclaim
        } else {
            if (write != read) {
                retire_list[write] = node;
            }
            ++write;
        }
    }

    retire_list.resize(write);
}

void EpochManager::force_reclaim_all() noexcept {
    for (ThreadState& state : thread_states_) {
        for (RetireNode& node : state.retire_list_) {
            node.deleter(node.ptr);
        }
        state.retire_list_.clear();
    }
}

} // namespace stratadb::memory
