#pragma once

#include "stratadb/utils/hardware.hpp"
#include "stratadb/wal/vyukov_mpsc_queue.hpp" // For the MpscNode base struct

#include <atomic>
#include <cstddef>
#include <thread>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h> // For _mm_pause()
#endif

namespace stratadb::wal {
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#endif
}

// Generates a dense, zero-cost thread ID (0, 1, 2, ...)
// so we can index into the mailbox array cleanly.
inline auto get_dense_thread_index() noexcept -> std::size_t {
    static std::atomic<std::size_t> global_counter{0};
    thread_local std::size_t tl_index = global_counter.fetch_add(1, std::memory_order_relaxed);
    return tl_index;
}

template <std::size_t Capacity = 256>
class alignas(utils::CACHE_LINE_SIZE) SpscRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    static constexpr std::size_t MASK = Capacity - 1;

  public:
    SpscRingBuffer() = default;

    [[nodiscard]] auto try_push(MpscNode* node) noexcept -> bool {
        const std::size_t head = head_.load(std::memory_order_acquire);
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t next_tail = (tail + 1) & MASK;

        if (next_tail == head) {
            return false; // Buffer is full
        }

        buffer_[tail] = node;
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    [[nodiscard]] auto try_pop() noexcept -> MpscNode* {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_acquire);

        if (head == tail) {
            return nullptr; // Buffer is empty
        }

        auto node = buffer_[head];
        head_.store((head + 1) & MASK, std::memory_order_release);
        return node;
    }

  private:
    // The consumer modifies head_. The producer modifies tail_.
    // We force them onto entirely separate physical CPU cache lines.
    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::size_t> head_{0};
    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::size_t> tail_{0};
    MpscNode* buffer_[Capacity]{};
};

} // namespace stratadb::wal