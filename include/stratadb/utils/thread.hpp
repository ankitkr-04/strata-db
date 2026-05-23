#pragma once

#include <atomic>
#include <cstddef>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace stratadb::utils {

// Emit a CPU pause / yield hint inside spin-wait loops.
//   x86-64 : PAUSE — prevents pipeline stalls from memory-order mis-speculation.
//   AArch64 : YIELD — hints the scheduler to prefer a sibling SMT thread.
//   Other   : no-op; compiles away with zero code generated.
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    _mm_pause();
#elif defined(__aarch64__)
    __asm__ volatile("yield" ::: "memory");
#endif
}

// Returns a dense, monotonically assigned per-thread index (0, 1, 2, …).
//
// The index is stable for the full lifetime of the calling thread; indices
// are NOT reused after thread exit.  Unlike std::this_thread::get_id() the
// returned value fits directly into a small array subscript with no hashing.
//
// Thread-safe: the global counter is an atomic fetch_add.
[[nodiscard]] inline auto get_dense_thread_index() noexcept -> std::size_t {
    static std::atomic<std::size_t> counter{0};
    thread_local const std::size_t index = counter.fetch_add(1, std::memory_order_relaxed);
    return index;
}

} // namespace stratadb::utils