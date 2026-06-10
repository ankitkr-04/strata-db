#pragma once

#include <atomic>

namespace stratadb::test {

template <typename T>
void do_not_optimize(const T& value) noexcept {
#if defined(__GNUC__) || defined(__clang__)
    __asm__ __volatile__("" : : "g"(value) : "memory");
#else
    (void)value;
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

struct DestructionSpy {
    DestructionSpy() = default;
    DestructionSpy(const DestructionSpy&) = default;
    DestructionSpy(DestructionSpy&&) = delete;
    auto operator=(const DestructionSpy&) -> DestructionSpy& = default;
    auto operator=(DestructionSpy&&) -> DestructionSpy& = delete;

    inline static std::atomic<int> count{0};

    static void reset() noexcept {
        count.store(0, std::memory_order_relaxed);
    }

    ~DestructionSpy() {
        count.fetch_add(1, std::memory_order_relaxed);
    }
};

} // namespace stratadb::test
