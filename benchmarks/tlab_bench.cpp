#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/utils/math.hpp"

#include <atomic>
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
        if (!std::has_single_bit(alignment)) {
            return reservation;
        }

        std::size_t old = offset_.load(std::memory_order_relaxed);
        while (true) {
            std::size_t aligned_offset = 0;
            if (!utils::align_up_checked(old, alignment, aligned_offset)) {
                return reservation;
            }

            if (capacity_ < size || aligned_offset > capacity_ - size) {
                return reservation;
            }

            ++reservation.stats.cas_attempts;
            const std::size_t next = aligned_offset + size;
            if (offset_.compare_exchange_weak(old, next, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                reservation.offset = aligned_offset;
                return reservation;
            }

            ++reservation.stats.cas_failures;
        }
    }

  private:
    std::size_t capacity_{0};
    std::atomic<std::size_t> offset_{0};
};

class InstrumentedTLAB {
  public:
    explicit InstrumentedTLAB(InstrumentedSharedCursorArena& arena) noexcept
        : arena_(arena) {}

    [[nodiscard]] auto allocate(std::size_t size, std::size_t alignment) noexcept -> AllocationStats {
        AllocationStats stats{};
        if (!std::has_single_bit(alignment)) {
            return stats;
        }

        if (current_offset_ != invalid_offset_ && current_offset_ <= block_end_) {
            const std::size_t aligned = utils::align_up_pow2(current_offset_, alignment);
            if (aligned >= current_offset_ && aligned <= block_end_) {
                const std::size_t remaining = block_end_ - aligned;
                if (remaining >= size) {
                    current_offset_ = aligned + size;
                    return stats;
                }
            }
        }

        return allocate_slow(size, alignment);
    }

  private:
    [[nodiscard]] auto allocate_slow(std::size_t size, std::size_t alignment) noexcept -> AllocationStats {
        AllocationStats stats{};

        if (size >= (kTLABSize / 2)) {
            auto direct = arena_.allocate_aligned(size, alignment);
            stats.cas_attempts += direct.stats.cas_attempts;
            stats.cas_failures += direct.stats.cas_failures;
            return stats;
        }

        if (size > std::numeric_limits<std::size_t>::max() - alignment) {
            return stats;
        }

        auto refill = arena_.allocate_block(size + alignment);
        stats.cas_attempts += refill.stats.cas_attempts;
        stats.cas_failures += refill.stats.cas_failures;
        if (refill.offset == invalid_offset_) {
            return stats;
        }

        current_offset_ = refill.offset;
        block_end_ = refill.offset + std::max(size + alignment, kTLABSize);
        if (!utils::align_up_checked(block_end_, kBlockAlignment, block_end_)) {
            return stats;
        }
        return stats + allocate(size, alignment);
    }

  private:
    static constexpr std::size_t invalid_offset_ = std::numeric_limits<std::size_t>::max();

    InstrumentedSharedCursorArena& arena_;
    std::size_t current_offset_{invalid_offset_};
    std::size_t block_end_{invalid_offset_};
};

struct WorkerStats {
    std::size_t cas_attempts{0};
    std::size_t cas_failures{0};
};

template <typename WorkerFactory>
void run_parallel_contention_iteration(int thread_count, WorkerFactory&& make_worker, WorkerStats& aggregate) {
    OneShotStartGate gate(thread_count);
    std::vector<std::thread> workers;
    std::vector<WorkerStats> results(static_cast<std::size_t>(thread_count));
    workers.reserve(static_cast<std::size_t>(thread_count));

    for (int thread_index = 0; thread_index < thread_count; ++thread_index) {
        workers.emplace_back([&, thread_index]() {
            auto worker = make_worker();
            gate.arrive_and_wait();
            WorkerStats local{};
            for (std::size_t i = 0; i < kOpsPerThread; ++i) {
                const auto stats = worker(kNodeSize, kAlignment);
                local.cas_attempts += stats.cas_attempts;
                local.cas_failures += stats.cas_failures;
            }
            results[static_cast<std::size_t>(thread_index)] = local;
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }

    for (const auto& result : results) {
        aggregate.cas_attempts += result.cas_attempts;
        aggregate.cas_failures += result.cas_failures;
    }
}

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

static void BM_SharedCursorCASPerAlloc(benchmark::State& state) {
    const int thread_count = static_cast<int>(state.range(0));
    const std::size_t capacity = static_cast<std::size_t>(thread_count) * kOpsPerThread * (kNodeSize + kAlignment + 16);

    WorkerStats aggregate{};
    state.SetLabel("arena_only_cas");

    for (auto _ : state) {
        (void)_;
        InstrumentedSharedCursorArena arena(capacity);
        run_parallel_contention_iteration(
            thread_count,
            [&arena]() {
                return [&arena](std::size_t size, std::size_t alignment) {
                    return arena.allocate_aligned(size, alignment).stats;
                };
            },
            aggregate);
    }

    finalize_contention_counters(state, thread_count, aggregate);
}

static void BM_TLABRefillCAS(benchmark::State& state) {
    const int thread_count = static_cast<int>(state.range(0));
    const std::size_t capacity =
        (static_cast<std::size_t>(thread_count) * kOpsPerThread * (kNodeSize + kAlignment + 16))
        + (static_cast<std::size_t>(thread_count) * kTLABSize);

    WorkerStats aggregate{};
    state.SetLabel("tlab_refill");

    for (auto _ : state) {
        (void)_;
        InstrumentedSharedCursorArena arena(capacity);
        run_parallel_contention_iteration(
            thread_count,
            [&arena]() {
                return [tlab = InstrumentedTLAB(arena)](std::size_t size, std::size_t alignment) mutable {
                    return tlab.allocate(size, alignment);
                };
            },
            aggregate);
    }

    finalize_contention_counters(state, thread_count, aggregate);
}

void apply_thread_args(benchmark::Benchmark* benchmark_handle) {
    for (int threads : {1, 2, 4, 8, 16}) {
        benchmark_handle->Arg(threads);
    }
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_SharedCursorCASPerAlloc)->Apply(stratadb::bench::apply_thread_args);
BENCHMARK(stratadb::bench::BM_TLABRefillCAS)->Apply(stratadb::bench::apply_thread_args);
