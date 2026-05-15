// benchmarks/tlab_bench.cpp
//
// StrataDB TLAB & Arena Contention Microbenchmarks
// Measures Compare-And-Swap (CAS) failure rates and atomic contention
// at the core allocator boundaries.

#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/utils/math.hpp"

#include <atomic>
#include <barrier>
#include <benchmark/benchmark.h>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <thread>
#include <vector>

namespace stratadb::bench {
namespace {

constexpr std::size_t kNodeSize = 256; // Standard representative allocation size
constexpr std::size_t kAlignment = memtable::SkipListNode::REQUIRED_ALIGNMENT;
constexpr std::size_t kTLABSize = config::MemoryConfig::DEFAULT_TLAB_SIZE;
constexpr std::size_t kBlockAlignment = config::MemoryConfig::DEFAULT_BLOCK_ALIGNMENT;

struct AllocationStats {
    std::size_t cas_attempts{0};
    std::size_t cas_failures{0};
};

struct CursorReservation {
    std::size_t offset{std::numeric_limits<std::size_t>::max()};
    std::size_t actual_size{0};
    AllocationStats stats{};
};

// InstrumentedSharedCursorArena
class InstrumentedSharedCursorArena {
  public:
    explicit InstrumentedSharedCursorArena(std::size_t capacity) noexcept
        : capacity_(capacity) {}

    [[nodiscard]] auto allocate_aligned(std::size_t size, std::size_t alignment) noexcept -> CursorReservation {
        return bump_allocate(size, alignment);
    }

    [[nodiscard]] auto allocate_block(std::size_t min_size) noexcept -> CursorReservation {
        std::size_t block_size = std::max(min_size, kTLABSize);
        if (!utils::align_up_checked(block_size, kBlockAlignment, block_size)) {
            return {};
        }
        return bump_allocate(block_size, kBlockAlignment);
    }

  private:
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
    [[nodiscard]] auto bump_allocate(std::size_t size, std::size_t alignment) noexcept -> CursorReservation {
        CursorReservation reservation{};
        if (!std::has_single_bit(alignment))
            return reservation;

        std::size_t old = offset_.load(std::memory_order_relaxed);
        while (true) {
            std::size_t aligned_offset = 0;
            if (!utils::align_up_checked(old, alignment, aligned_offset))
                return reservation;
            if (capacity_ < size || aligned_offset > capacity_ - size)
                return reservation;

            ++reservation.stats.cas_attempts;
            const std::size_t next = aligned_offset + size;

            if (offset_.compare_exchange_weak(old, next, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                reservation.offset = aligned_offset;
                reservation.actual_size = size;
                return reservation;
            }
            ++reservation.stats.cas_failures;
        }
    }

    std::size_t capacity_{0};
    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::size_t> offset_{0};
};

// InstrumentedTLAB
class InstrumentedTLAB {
  public:
    explicit InstrumentedTLAB(InstrumentedSharedCursorArena& arena) noexcept
        : arena_(arena) {}

    [[nodiscard]] auto allocate(std::size_t size, std::size_t alignment) noexcept -> AllocationStats {
        AllocationStats stats{};
        if (!std::has_single_bit(alignment))
            return stats;

        // Fast Path
        const std::size_t aligned = utils::align_up_pow2(current_offset_, alignment);
        if (current_offset_ != invalid_offset_ && aligned >= current_offset_ && aligned <= block_end_) {
            const std::size_t remaining = block_end_ - aligned;
            if (remaining >= size) {
                current_offset_ = aligned + size;
                return stats;
            }
        }

        // Oversized Allocation Fallback
        if (size >= (kTLABSize / 2)) {
            auto direct = arena_.allocate_aligned(size, alignment);
            stats.cas_attempts += direct.stats.cas_attempts;
            stats.cas_failures += direct.stats.cas_failures;
            return stats;
        }

        // Block Refill
        if (size > std::numeric_limits<std::size_t>::max() - alignment)
            return stats;

        auto refill = arena_.allocate_block(kTLABSize);
        stats.cas_attempts += refill.stats.cas_attempts;
        stats.cas_failures += refill.stats.cas_failures;

        if (refill.offset == invalid_offset_)
            return stats;

        // Reset offsets cleanly with actual consumed size
        const std::size_t new_aligned = utils::align_up_pow2(refill.offset, alignment);
        current_offset_ = new_aligned + size;
        block_end_ = refill.offset + refill.actual_size;

        return stats;
    }

  private:
    static constexpr std::size_t invalid_offset_ = std::numeric_limits<std::size_t>::max();

    InstrumentedSharedCursorArena& arena_;
    std::size_t current_offset_{invalid_offset_};
    std::size_t block_end_{invalid_offset_};
};

// False-Sharing Quarantined Stats
struct alignas(utils::CACHE_LINE_SIZE) WorkerStats {
    std::size_t cas_attempts{0};
    std::size_t cas_failures{0};
};
static_assert(sizeof(WorkerStats) % utils::CACHE_LINE_SIZE == 0, "WorkerStats risks false sharing");

template <bool UseTLAB>
void run_contention_benchmark(benchmark::State& state, std::string_view label) {
    const int thread_count = static_cast<int>(state.range(0));
    const auto tc = static_cast<std::size_t>(thread_count);

    // Leverage dynamic runtime configuration from benchmark_common.hpp
    // Defaulting to 65536 for contention saturation unless overridden.
    const std::size_t ops = effective_ops(65536);

    const std::size_t capacity_per_iter = (tc * ops * (kNodeSize + kAlignment + 16)) + (UseTLAB ? (tc * kTLABSize) : 0);

    state.SetLabel(std::string(label));

    InstrumentedSharedCursorArena* shared_arena = nullptr;
    std::vector<WorkerStats> results(tc);

    bool stop_flag = false;
    std::barrier<> setup_barrier(thread_count + 1);
    std::barrier<> go_barrier(thread_count + 1);
    std::barrier<> done_barrier(thread_count + 1);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int thread_index = 0; thread_index < thread_count; ++thread_index) {
        workers.emplace_back([&, t = static_cast<std::size_t>(thread_index)]() {
            while (true) {

                setup_barrier.arrive_and_wait();
                if (stop_flag)
                    break;

                // TLAB instantiated locally per iteration if needed
                std::optional<InstrumentedTLAB> tlab;
                if constexpr (UseTLAB) {
                    tlab.emplace(*shared_arena);
                }

                // Registers for local accumulation (bypasses memory writes in hot loop)
                std::size_t local_attempts = 0;
                std::size_t local_failures = 0;

                // Edge of timed window
                go_barrier.arrive_and_wait();

                // Pure Hot Path
                for (std::size_t i = 0; i < ops; ++i) {
                    if constexpr (UseTLAB) {
                        const auto s = tlab->allocate(kNodeSize, kAlignment);
                        local_attempts += s.cas_attempts;
                        local_failures += s.cas_failures;
                    } else {
                        const auto s = shared_arena->allocate_aligned(kNodeSize, kAlignment);
                        local_attempts += s.stats.cas_attempts;
                        local_failures += s.stats.cas_failures;
                    }
                }

                // Finalize
                results[t].cas_attempts = local_attempts;
                results[t].cas_failures = local_failures;
                done_barrier.arrive_and_wait();
            }
        });
    }

    WorkerStats aggregate{};

    for (auto _ : state) {
        state.PauseTiming();

        InstrumentedSharedCursorArena arena(capacity_per_iter);
        shared_arena = &arena;

        setup_barrier.arrive_and_wait();

        state.ResumeTiming();
        go_barrier.arrive_and_wait();   // GO
        done_barrier.arrive_and_wait(); // DONE
        state.PauseTiming();

        for (const auto& r : results) {
            aggregate.cas_attempts += r.cas_attempts;
            aggregate.cas_failures += r.cas_failures;
        }

        state.ResumeTiming();
    }

    state.PauseTiming();
    stop_flag = true;
    setup_barrier.arrive_and_wait();

    for (auto& w : workers)
        w.join();

    const auto total_ops = static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(thread_count)
                           * static_cast<std::int64_t>(ops);
    state.SetItemsProcessed(total_ops);
    state.counters["total_atomic_ops"] = static_cast<double>(aggregate.cas_attempts);
    state.counters["failed_cas_retries"] = static_cast<double>(aggregate.cas_failures);
    state.counters["atomic_ops_per_alloc"] =
        (total_ops == 0) ? 0.0 : static_cast<double>(aggregate.cas_attempts) / static_cast<double>(total_ops);
    state.counters["failed_cas_per_alloc"] =
        (total_ops == 0) ? 0.0 : static_cast<double>(aggregate.cas_failures) / static_cast<double>(total_ops);
}

// Measures contention when every thread hits the central atomic offset.
static void BM_SharedCursorCASPerAlloc(benchmark::State& state) {
    run_contention_benchmark<false>(state, "arena_only_cas");
}

// Measures contention massively amortized by thread-local blocks.
static void BM_TLABRefillCAS(benchmark::State& state) {
    run_contention_benchmark<true>(state, "tlab_refill");
}

void apply_thread_args(benchmark::Benchmark* b) {
    for_each_supported_thread_count([&](int threads) { b->Arg(threads); });
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_SharedCursorCASPerAlloc)->Apply(stratadb::bench::apply_thread_args)->UseRealTime();
BENCHMARK(stratadb::bench::BM_TLABRefillCAS)->Apply(stratadb::bench::apply_thread_args)->UseRealTime();