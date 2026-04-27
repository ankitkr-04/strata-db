#include "stratadb/memory/epoch_manager.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <cstdio>
#include <thread>

namespace stratadb::memory {
namespace {
// Support up to 64 StratDB instances in the same process (tuning parameter)
constexpr std::size_t MAX_DB_INSTANCES = 64;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<std::size_t> global_instance_counter{0};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
thread_local std::array<std::size_t, MAX_DB_INSTANCES> tl_epoch_slots = [] {
    std::array<std::size_t, MAX_DB_INSTANCES> slots{};
    slots.fill(std::numeric_limits<std::size_t>::max());
    return slots;
}();

} // namespace

EpochManager::EpochManager() noexcept
    : instance_id_(global_instance_counter.fetch_add(1, std::memory_order_relaxed)) {

    if (instance_id_ >= MAX_DB_INSTANCES) {
        std::fputs("EpochManager instance limit exceeded\n", stderr);
        std::terminate();
    }
}

EpochManager::~EpochManager() noexcept {
    force_reclaim_all();
}

auto EpochManager::my_thread_index() const noexcept -> std::size_t {
    return tl_epoch_slots[instance_id_];
}

auto EpochManager::register_thread() noexcept -> std::expected<void, EpochError> {
    // Each thread must register exactly once
    assert(thread_index_ == INVALID_THREAD);
    if (thread_index_ != INVALID_THREAD) {
        std::terminate(); // state is already corrupted, cannot continue
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
                return {};
            }
        }
    }

    return std::unexpected(EpochError::ThreadLimitExceeded);
}

void EpochManager::unregister_thread() {
    const std::size_t slot = my_thread_index();
    assert(slot != INVALID_THREAD);

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
    const std::size_t slot = my_thread_index();
    assert(slot != INVALID_THREAD);
    std::uint64_t e = global_epoch_.load(std::memory_order_acquire);

    thread_states_[slot].state.store(e, std::memory_order_release);
}

void EpochManager::leave() noexcept {
    const std::size_t slot = my_thread_index();
    assert(slot != INVALID_THREAD);
    thread_states_[slot].state.store(INACTIVE_EPOCH, std::memory_order_release);
}

auto EpochManager::current_epoch() const noexcept -> std::uint64_t {
    return global_epoch_.load(std::memory_order_acquire);
}

void EpochManager::try_advance_epoch() noexcept {
    // Advance global epoch (logical time)
    // acq_rel ensures ordering with readers/writers
    std::uint64_t current = global_epoch_.load(std::memory_order_relaxed);
    global_epoch_.compare_exchange_strong(current, current + 1, std::memory_order_release, std::memory_order_relaxed);
}

void EpochManager::quiescent_reclaim() noexcept {
    assert(my_thread_index() != INVALID_THREAD);
    try_advance_epoch();
    reclaim();
}
void EpochManager::retire_node(void* ptr, void (*deleter)(void*)) noexcept {
    const std::size_t slot = my_thread_index();
    assert(slot != INVALID_THREAD);
    auto& state = thread_states_[slot];

    // Tag node with current epoch
    // Meaning: may still be visible to threads in ≤ this epoch
    std::uint64_t retire_epoch = global_epoch_.load(std::memory_order_acquire);
    RetireNode node{.ptr = ptr, .deleter = deleter, .retire_epoch = retire_epoch};

    // Fast path: Push to L1 cache slab
    if (state.slab_count < ThreadState::SLAB_CAPACITY) {
        state.slab[state.slab_count++] = node;

    } else {
        // Slow path: Backpressure via overflow list
        if (state.overflow_count == ThreadState::SLAB_CAPACITY) {
            std::uint32_t new_capacity = state.overflow_capacity == 0 ? 256 : state.overflow_capacity * 2;
            auto* new_overflow = new (std::nothrow) RetireNode[new_capacity];
            if (!new_overflow) {
                std::fputs("Failed to allocate overflow retire list\n", stderr);
                std::terminate();
            }
            if (state.overflow) {
                std::copy(state.overflow, state.overflow + state.overflow_count, new_overflow);
                delete[] state.overflow;
            }
            state.overflow = new_overflow;
            state.overflow_capacity = new_capacity;
        }
        state.overflow[state.overflow_count++] = node;
    }

    const std::size_t total_retired = state.slab_count + state.overflow_count;

    if ((total_retired & RECLAIM_INTERVAL_MASK) == 0) [[unlikely]] {
        try_advance_epoch();
        reclaim();

        static thread_local std::uint32_t backoff_us = 1;
        // Backpressure: if too many unreclaimed nodes, yield to give other threads a chance to reclaim
        if (total_retired > RETIRE_LIST_THRESHOLD) [[unlikely]] {

            std::this_thread::sleep_for(std::chrono::microseconds(backoff_us));
            backoff_us = std::min(backoff_us * 2u, 1000u); // cap backoff at 1ms

            if (total_retired > RETIRE_LIST_THRESHOLD * 4) {
                reclaim(); // if still too high, do a synchronous reclaim before yielding again
            }

        } else {
            backoff_us = 1;
        }
    }
}

void EpochManager::reclaim() noexcept {
    const std::size_t slot = my_thread_index();
    assert(slot != INVALID_THREAD);

    std::uint64_t min_epoch = global_epoch_.load(std::memory_order_acquire);

    for (std::size_t word_index = 0; word_index < ACTIVE_THREAD_MASK_WORDS; ++word_index) {
        std::uint64_t active_bits = active_thread_masks_[word_index].bits.load(std::memory_order_acquire);

        while (active_bits != 0) {
            const auto bit_index = static_cast<std::size_t>(std::countr_zero(active_bits));
            const std::size_t active_slot = (word_index * 64) + bit_index;
            const std::uint64_t e = thread_states_[active_slot].state.load(std::memory_order_acquire);

            if (e != INACTIVE_EPOCH) {
                min_epoch = std::min(min_epoch, e);
            }

            active_bits &= active_bits - 1;
        }
    }

    auto& state = thread_states_[slot];

    // --- COMPACT SLAB (Outer Loop 1) ---
    std::uint32_t write_idx = 0;
    for (std::uint32_t i = 0; i < state.slab_count; ++i) {
        if (state.slab[i].retire_epoch < min_epoch) {
            state.slab[i].deleter(state.slab[i].ptr);
        } else {
            state.slab[write_idx] = state.slab[i];
            ++write_idx;
        }
    }
    state.slab_count = write_idx; //Outside the loop

    
    if (state.overflow_count > 0) {
        write_idx = 0;
        for (std::uint32_t i = 0; i < state.overflow_count; ++i) {
            if (state.overflow[i].retire_epoch < min_epoch) {
                state.overflow[i].deleter(state.overflow[i].ptr);
            } else {
                state.overflow[write_idx++] = state.overflow[i];
            }
        }
        state.overflow_count = write_idx; // Outside the loop
    }
}
void EpochManager::force_reclaim_all() noexcept {
    for (ThreadState& state : thread_states_) {
        for (std::uint32_t i = 0; i < state.slab_count; ++i) {
            state.slab[i].deleter(state.slab[i].ptr);
        }
        state.slab_count = 0;

        for (std::uint32_t i = 0; i < state.overflow_count; ++i) {
            state.overflow[i].deleter(state.overflow[i].ptr);
        }
        state.overflow_count = 0;

        if (state.overflow) {
            delete[] state.overflow;
            state.overflow = nullptr;
            state.overflow_capacity = 0;
        }
    }
}

} // namespace stratadb::memory
