#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/utils/math.hpp"

#include <atomic>
#include <barrier>
#include <benchmark/benchmark.h>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <thread>
#include <vector>

namespace stratadb::bench {
namespace {

constexpr std::size_t kNodeSize = 256;
constexpr std::size_t kAlignment = memtable::SkipListNode::REQUIRED_ALIGNMENT;
constexpr std::size_t kOpsPerThread = 65536;
constexpr std::size_t kTLABSize = config::MemoryConfig::DEFAULT_TLAB_SIZE;
constexpr std::size_t kBlockAlignment = config::MemoryConfig::DEFAULT_BLOCK_ALIGNMENT;

struct AllocationStats {
    std::size_t cas_attempts{0};
    std::size_t cas_failures{0};
};

[[nodiscard]] auto operator+(AllocationStats lhs, const AllocationStats& rhs) noexcept -> AllocationStats {
    lhs.cas_attempts += rhs.cas_attempts;
    lhs.cas_failures += rhs.cas_failures;
    return lhs;
}

struct CursorReservation {
    std::size_t offset{std::numeric_limits<std::size_t>::max()};
    AllocationStats stats{};
};

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
                return reservation;
            }
            ++reservation.stats.cas_failures;
        }
    }

    std::size_t capacity_{0};
    std::atomic<std::size_t> offset_{0};
};

class InstrumentedTLAB {
  public:
    explicit InstrumentedTLAB(InstrumentedSharedCursorArena& arena) noexcept
        : arena_(arena) {}

    [[nodiscard]] auto allocate(std::size_t size, std::size_t alignment) noexcept -> AllocationStats {
        AllocationStats stats{};
        if (!std::has_single_bit(alignment))
            return stats;

        if (current_offset_ != invalid_offset_ && current_offset_ <= block_end_) {
            const std::size_t aligned = utils::align_up_pow2(current_offset_, alignment);
            if (aligned >= current_offset_ && aligned <= block_end_) {
                const std::size_t remaining = block_end_ - aligned;
                if (remaining >= size) {
                    current_offset_ = aligned + size;
                    return stats; // fast path: zero CAS
                }
            }
        }

        return allocate_slow(size, alignment);
    }

  private:
    [[nodiscard]] auto allocate_slow(std::size_t size, std::size_t alignment) noexcept -> AllocationStats {
        AllocationStats stats{};

        if (size >= (kTLABSize / 2)) {
            // Oversized: go directly to the arena.
            auto direct = arena_.allocate_aligned(size, alignment);
            stats.cas_attempts += direct.stats.cas_attempts;
            stats.cas_failures += direct.stats.cas_failures;
            return stats;
        }

        if (size > std::numeric_limits<std::size_t>::max() - alignment)
            return stats;

        auto refill = arena_.allocate_block(kTLABSize);
        stats.cas_attempts += refill.stats.cas_attempts;
        stats.cas_failures += refill.stats.cas_failures;
        if (refill.offset == invalid_offset_)
            return stats;

        current_offset_ = refill.offset;
        block_end_ = refill.offset + kTLABSize;

        return stats + allocate(size, alignment);
    }

    static constexpr std::size_t invalid_offset_ = std::numeric_limits<std::size_t>::max();

    InstrumentedSharedCursorArena& arena_;
    std::size_t current_offset_{invalid_offset_};
    std::size_t block_end_{invalid_offset_};
};

struct WorkerStats {
    std::size_t cas_attempts{0};
    std::size_t cas_failures{0};
};

void finalize_contention_counters(benchmark::State& state, int thread_count, const WorkerStats& aggregate) {
    const auto total_ops = static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(thread_count)
                           * static_cast<std::int64_t>(kOpsPerThread);

    state.SetItemsProcessed(total_ops);
    state.counters["total_atomic_ops"] = static_cast<double>(aggregate.cas_attempts);
    state.counters["failed_cas_retries"] = static_cast<double>(aggregate.cas_failures);
    state.counters["atomic_ops_per_alloc"] =
        (total_ops == 0) ? 0.0 : static_cast<double>(aggregate.cas_attempts) / static_cast<double>(total_ops);
    state.counters["failed_cas_per_alloc"] =
        (total_ops == 0) ? 0.0 : static_cast<double>(aggregate.cas_failures) / static_cast<double>(total_ops);
}

template <typename WorkerFactory>
void run_contention_benchmark(benchmark::State& state,
                              std::string_view label,
                              // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                              int thread_count,
                              std::size_t arena_capacity_per_iter,
                              WorkerFactory&& factory) {
    state.SetLabel(std::string(label));

    const auto tc = static_cast<std::size_t>(thread_count);

    // Per-iteration arena pointer — written by main in PauseTiming, read by
    // workers after start_barrier.
    std::atomic<InstrumentedSharedCursorArena*> shared_arena{nullptr};
    std::vector<WorkerStats> results(tc);

    std::atomic<bool> stop_flag{false};
    std::barrier<> start_barrier(thread_count + 1);
    std::barrier<> done_barrier(thread_count + 1);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int thread_index = 0; thread_index < thread_count; ++thread_index) {
        workers.emplace_back([&, thread_index]() -> auto {
            const auto ti = static_cast<std::size_t>(thread_index);

            while (true) {
                start_barrier.arrive_and_wait();
                if (stop_flag.load(std::memory_order_acquire))
                    break;

                // Build a fresh worker functor (e.g. a new TLAB) referencing
                // this iteration's arena — cheap, and outside timed window
                // because we're between the two barriers here.
                auto* arena = shared_arena.load(std::memory_order_acquire);
                auto worker = factory(*arena);

                WorkerStats local{};
                for (std::size_t i = 0; i < kOpsPerThread; ++i) {
                    const auto s = worker(kNodeSize, kAlignment);
                    local.cas_attempts += s.cas_attempts;
                    local.cas_failures += s.cas_failures;
                }
                results[ti] = local;

                done_barrier.arrive_and_wait();
            }
        });
    }

    WorkerStats aggregate{};

    for (auto _ : state) {
        (void)_;

        // ------ PAUSED: construct a fresh arena for this iteration ---------
        state.PauseTiming();

        InstrumentedSharedCursorArena arena(arena_capacity_per_iter);
        shared_arena.store(&arena, std::memory_order_release);

        state.ResumeTiming();
        // ------ TIMING: only CAS / bump-pointer work happens here ----------

        start_barrier.arrive_and_wait(); // release workers
        done_barrier.arrive_and_wait();  // wait for workers

        // ------ PAUSED: accumulate stats -----------------------------------
        state.PauseTiming();

        for (const auto& r : results) {
            aggregate.cas_attempts += r.cas_attempts;
            aggregate.cas_failures += r.cas_failures;
        }

        state.ResumeTiming();
    }

    stop_flag.store(true, std::memory_order_release);
    start_barrier.arrive_and_wait();
    for (auto& w : workers)
        w.join();

    finalize_contention_counters(state, thread_count, aggregate);
}

// ---------------------------------------------------------------------------
// BM_SharedCursorCASPerAlloc — every allocation hits the shared atomic cursor.
// ---------------------------------------------------------------------------
static void BM_SharedCursorCASPerAlloc(benchmark::State& state) {
    const int thread_count = static_cast<int>(state.range(0));
    const std::size_t capacity = static_cast<std::size_t>(thread_count) * kOpsPerThread * (kNodeSize + kAlignment + 16);

    run_contention_benchmark(state, "arena_only_cas", thread_count, capacity, [](InstrumentedSharedCursorArena& arena) {
        return [&arena](std::size_t size, std::size_t alignment) {
            return arena.allocate_aligned(size, alignment).stats;
        };
    });
}

static void BM_TLABRefillCAS(benchmark::State& state) {
    const int thread_count = static_cast<int>(state.range(0));
    const std::size_t capacity =
        (static_cast<std::size_t>(thread_count) * kOpsPerThread * (kNodeSize + kAlignment + 16))
        + (static_cast<std::size_t>(thread_count) * kTLABSize);

    run_contention_benchmark(state, "tlab_refill", thread_count, capacity, [](InstrumentedSharedCursorArena& arena) {
        // InstrumentedTLAB is move-constructible (stores a ref), so we
        // capture it by value inside the returned lambda.
        return [tlab = InstrumentedTLAB(arena)](std::size_t size, std::size_t alignment) mutable {
            return tlab.allocate(size, alignment);
        };
    });
}

void apply_thread_args(benchmark::Benchmark* b) {
    for (int threads : {1, 2, 4, 8, 16}) {
        b->Arg(threads);
    }
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_SharedCursorCASPerAlloc)->Apply(stratadb::bench::apply_thread_args)->UseRealTime();
BENCHMARK(stratadb::bench::BM_TLABRefillCAS)->Apply(stratadb::bench::apply_thread_args)->UseRealTime();