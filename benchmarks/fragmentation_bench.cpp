// benchmarks/fragmentation_bench.cpp
//
// Fragmentation degradation proof:
//   - system heap repeatedly malloc/free's mixed payload sizes
//   - StrataDB TLAB repeatedly allocates mixed payload sizes and recycles whole arenas
//   - both report first-decile and last-decile p99 allocation latency

#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"

#include <algorithm>
#include <benchmark/benchmark.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

namespace stratadb::bench {
namespace {

using Clock = std::chrono::steady_clock;

constexpr std::size_t kDefaultOps = 1'000'000;
constexpr std::size_t kLiveSlots = 65'536;
constexpr std::size_t kSampleStride = 256;
constexpr std::size_t kArenaBytes = 256ULL << 20; // 256 MiB

struct FastRng {
    std::uint64_t state{0x9e37'79b9'7f4a'7c15ULL};

    [[nodiscard]] auto next() noexcept -> std::uint64_t {
        std::uint64_t x = state;
        x ^= x << 7;
        x ^= x >> 9;
        state = x;
        return x;
    }
};

[[nodiscard]] auto chaotic_size(std::uint64_t r) noexcept -> std::size_t {
    static constexpr std::array<std::size_t, 3> kSizes{128, 256, 1024};
    return kSizes[static_cast<std::size_t>(r % kSizes.size())];
}

[[nodiscard]] auto p99(std::vector<double> samples) -> double {
    return summarize_ext(std::move(samples)).p99_ns;
}

struct FragmentationStats {
    std::size_t ops_completed{0};
    std::size_t allocation_failures{0};
    std::size_t arena_resets{0};
    double first_p99_ns{0.0};
    double last_p99_ns{0.0};
};

template <typename AllocateFn>
[[nodiscard]] auto run_fragmentation_loop(std::size_t ops, AllocateFn allocate) -> FragmentationStats {
    FragmentationStats stats{};
    FastRng rng;

    const std::size_t first_cutoff = std::max<std::size_t>(1, ops / 10);
    const std::size_t last_start = ops - first_cutoff;
    std::vector<double> first_samples;
    std::vector<double> last_samples;
    first_samples.reserve(first_cutoff / kSampleStride + 1);
    last_samples.reserve(first_cutoff / kSampleStride + 1);

    for (std::size_t op = 0; op < ops; ++op) {
        const std::uint64_t r = rng.next();
        const std::size_t size = chaotic_size(r);
        const bool in_first_decile = op < first_cutoff;
        const bool in_last_decile = op >= last_start;
        const bool sample =
            (in_first_decile || in_last_decile) && (((op & (kSampleStride - 1)) == 0) || op == last_start);
        const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

        void* ptr = allocate(op, r, size, stats);

        if (sample) {
            const double ns = std::chrono::duration<double, std::nano>(Clock::now() - t0).count();
            if (in_first_decile) {
                first_samples.push_back(ns);
            } else {
                last_samples.push_back(ns);
            }
        }

        if (!ptr) {
            ++stats.allocation_failures;
            break;
        }

        std::memset(ptr, static_cast<int>(r & 0xffULL), size);
        benchmark::DoNotOptimize(ptr);
        ++stats.ops_completed;
    }

    stats.first_p99_ns = p99(std::move(first_samples));
    stats.last_p99_ns = p99(std::move(last_samples));
    return stats;
}

static void BM_SystemHeapFragmentation(benchmark::State& state) {
    const std::size_t ops = effective_ops(kDefaultOps);
    FragmentationStats aggregate{};

    for (auto _ : state) {
        std::vector<void*> live(kLiveSlots, nullptr);

        auto stats = run_fragmentation_loop(

            ops,
            // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
            [&](std::size_t op, std::uint64_t r, std::size_t size, FragmentationStats&) noexcept -> void* {
                const std::size_t slot = static_cast<std::size_t>((r >> 16) & (kLiveSlots - 1));
                if (live[slot]) {
                    std::free(live[slot]);
                    live[slot] = nullptr;
                }

                void* ptr = std::malloc(size);
                live[slot] = ptr;

                if ((op & 3ULL) == 0) {
                    const std::size_t victim = static_cast<std::size_t>((r >> 32) & (kLiveSlots - 1));
                    if (live[victim]) {
                        std::free(live[victim]);
                        live[victim] = nullptr;
                    }
                }

                return ptr;
            });

        state.PauseTiming();
        for (void* ptr : live) {
            std::free(ptr);
        }
        aggregate.ops_completed += stats.ops_completed;
        aggregate.allocation_failures += stats.allocation_failures;
        aggregate.first_p99_ns += stats.first_p99_ns;
        aggregate.last_p99_ns += stats.last_p99_ns;
        state.ResumeTiming();

        if (stats.allocation_failures != 0) {
            state.SkipWithError("malloc failed during fragmentation benchmark");
            break;
        }
    }

    const double denom = static_cast<double>(std::max<std::int64_t>(1, state.iterations()));
    state.SetItemsProcessed(static_cast<std::int64_t>(aggregate.ops_completed));
    state.counters["first_10pct_p99_ns"] = aggregate.first_p99_ns / denom;
    state.counters["last_10pct_p99_ns"] = aggregate.last_p99_ns / denom;
    state.counters["p99_degradation_ratio"] =
        aggregate.first_p99_ns == 0.0 ? 0.0 : aggregate.last_p99_ns / aggregate.first_p99_ns;
    state.counters["ops_per_iteration"] = static_cast<double>(ops);
}

static void BM_ArenaChurnStable(benchmark::State& state) {
    const std::size_t ops = effective_ops(kDefaultOps);
    FragmentationStats aggregate{};

    for (auto _ : state) {
        state.PauseTiming();
        auto exp = memory::Arena::create(make_memory_cfg(kArenaBytes));
        if (!exp.has_value()) {
            state.SkipWithError("Arena::create failed during fragmentation benchmark");
            state.ResumeTiming();
            break;
        }
        auto arena = std::make_unique<memory::Arena>(std::move(exp.value()));
        auto tlab = std::make_unique<memory::TLAB>(*arena);
        state.ResumeTiming();

        auto stats =
            run_fragmentation_loop(ops,
                                   [&](std::size_t, std::uint64_t, std::size_t size, FragmentationStats& loop_stats)
                                       -> void* {
                                       void* ptr = tlab->allocate(size, alignof(std::max_align_t));
                                       if (ptr) {
                                           return ptr;
                                       }

                                       tlab->detach();
                                       arena->reset();
                                       ++loop_stats.arena_resets;
                                       tlab = std::make_unique<memory::TLAB>(*arena);
                                       return tlab->allocate(size, alignof(std::max_align_t));
                                   });

        state.PauseTiming();
        aggregate.ops_completed += stats.ops_completed;
        aggregate.allocation_failures += stats.allocation_failures;
        aggregate.arena_resets += stats.arena_resets;
        aggregate.first_p99_ns += stats.first_p99_ns;
        aggregate.last_p99_ns += stats.last_p99_ns;
        state.ResumeTiming();

        if (stats.allocation_failures != 0) {
            state.SkipWithError("TLAB allocation failed during fragmentation benchmark");
            break;
        }
    }

    const double denom = static_cast<double>(std::max<std::int64_t>(1, state.iterations()));
    state.SetItemsProcessed(static_cast<std::int64_t>(aggregate.ops_completed));
    state.counters["first_10pct_p99_ns"] = aggregate.first_p99_ns / denom;
    state.counters["last_10pct_p99_ns"] = aggregate.last_p99_ns / denom;
    state.counters["p99_degradation_ratio"] =
        aggregate.first_p99_ns == 0.0 ? 0.0 : aggregate.last_p99_ns / aggregate.first_p99_ns;
    state.counters["arena_resets"] = static_cast<double>(aggregate.arena_resets);
    state.counters["ops_per_iteration"] = static_cast<double>(ops);
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_SystemHeapFragmentation)->UseRealTime();
BENCHMARK(stratadb::bench::BM_ArenaChurnStable)->UseRealTime();
