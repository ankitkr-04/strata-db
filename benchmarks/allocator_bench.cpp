#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"

#include <atomic>
#include <barrier>
#include <benchmark/benchmark.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <optional>
#include <thread>
#include <vector>

namespace stratadb::bench {
namespace {

using Clock = std::chrono::steady_clock;
constexpr std::size_t kPerThreadTargetBytes = 4ULL * 1024ULL * 1024ULL;
constexpr std::size_t kMinimumOpsPerThread = 4096;
constexpr std::size_t kLatencySampleMask = 31;

enum class AllocationMode : std::uint8_t {
    BaselineNew,
    ArenaOnly,
    ArenaWithTLAB,
};

struct WorkerResult {
    std::vector<void*> owned_allocations;
    std::vector<double> latency_samples_ns;
    bool allocation_failed{false};
};

[[nodiscard]] auto ops_per_thread(const NodeProfile& profile) noexcept -> std::size_t {
    const std::size_t size = profile.allocation_size();
    const std::size_t target = (size == 0) ? kMinimumOpsPerThread : (kPerThreadTargetBytes / size);
    return std::max(kMinimumOpsPerThread, target);
}

[[nodiscard]] auto make_arena_config(std::size_t bytes) noexcept -> config::MemoryConfig {
    config::MemoryConfig cfg;
    cfg.page_strategy = config::PageStrategy::Standard4K;
    cfg.prefault_on_init = false;
    cfg.total_budget_bytes = bytes;
    cfg.tlab_size_bytes = config::MemoryConfig::DEFAULT_TLAB_SIZE;
    cfg.block_alignment_bytes = config::MemoryConfig::DEFAULT_BLOCK_ALIGNMENT;
    return cfg;
}

void cleanup_owned_allocations(std::vector<WorkerResult>& results) noexcept {
    for (auto& result : results) {
        for (void* ptr : result.owned_allocations) {
            ::operator delete(ptr, std::align_val_t(memtable::SkipListNode::REQUIRED_ALIGNMENT));
        }
        result.owned_allocations.clear();
    }
}

// ---------------------------------------------------------------------------
// run_benchmark
//
// The key invariant: std::thread objects are created exactly once, before the
// Google Benchmark iteration loop.  Each iteration uses two std::barrier
// phases:
//
//   start_barrier  — main signals workers to begin; workers begin.
//   done_barrier   — workers signal completion; main collects stats.
//
// Only the code between start_barrier.arrive_and_wait() (main side) and
// done_barrier.arrive_and_wait() (main side) is inside the timed window.
// ---------------------------------------------------------------------------
void run_benchmark(benchmark::State& state, AllocationMode mode, std::string_view mode_name) {
    const int thread_count = static_cast<int>(state.range(0));
    const auto& profile = profile_from_index(static_cast<int>(state.range(1)));
    const std::size_t per_thread_ops = ops_per_thread(profile);
    const std::size_t node_size = profile.allocation_size();

    state.SetLabel(std::string(mode_name) + "/" + profile.name);

    // Build payloads once; moving this outside the loop prevents make_payload()
    // heap allocations from appearing in the allocator timing.
    const auto tc = static_cast<std::size_t>(thread_count);
    std::vector<std::string> thread_keys(tc);
    std::vector<std::string> thread_values(tc);
    for (int t = 0; t < thread_count; ++t) {
        const auto ti = static_cast<std::size_t>(t);
        thread_keys[ti] = make_payload(profile.key_bytes, static_cast<char>('a' + (t % 26)));
        thread_values[ti] = make_payload(profile.value_bytes, static_cast<char>('k' + (t % 10)));
    }

    // Shared iteration state.  Written by main under PauseTiming, read by
    // workers after start_barrier.
    std::atomic<memory::Arena*> shared_arena{nullptr};
    std::vector<WorkerResult> results(tc);

    std::atomic<bool> stop_flag{false};

    // +1 so the main thread participates in both barriers.
    std::barrier<> start_barrier(thread_count + 1);
    std::barrier<> done_barrier(thread_count + 1);

    // -----------------------------------------------------------------------
    // Thread pool — created once, torn down after the benchmark loop.
    // -----------------------------------------------------------------------
    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int thread_index = 0; thread_index < thread_count; ++thread_index) {
        workers.emplace_back([&, thread_index]() {
            const std::size_t ti = static_cast<std::size_t>(thread_index);
            const std::string& key = thread_keys[ti];
            const std::string& val = thread_values[ti];

            while (true) {
                // Wait for main to set up the iteration and call ResumeTiming.
                start_barrier.arrive_and_wait();
                if (stop_flag.load(std::memory_order_acquire))
                    break;

                auto& result = results[ti];
                result.owned_allocations.clear();
                result.owned_allocations.reserve(per_thread_ops);
                result.latency_samples_ns.clear();
                result.latency_samples_ns.reserve((per_thread_ops / (kLatencySampleMask + 1)) + 1);
                result.allocation_failed = false;

                // Construct TLAB per-iteration (trivially cheap — stores a ref).
                memory::Arena* arena = shared_arena.load(std::memory_order_acquire);
                std::optional<memory::TLAB> tlab;
                if (mode == AllocationMode::ArenaWithTLAB) {
                    tlab.emplace(*arena); // arena is non-null when mode requires it
                }

                for (std::size_t op = 0; op < per_thread_ops; ++op) {
                    void* mem = nullptr;
                    const bool sample = (op & kLatencySampleMask) == 0;
                    const auto t_start = sample ? Clock::now() : Clock::time_point{};

                    switch (mode) {
                        case AllocationMode::BaselineNew:
                            mem = ::operator new(node_size,
                                                 std::align_val_t(memtable::SkipListNode::REQUIRED_ALIGNMENT),
                                                 std::nothrow);
                            break;
                        case AllocationMode::ArenaOnly:
                            mem = arena->allocate_aligned(node_size, memtable::SkipListNode::REQUIRED_ALIGNMENT);
                            break;
                        case AllocationMode::ArenaWithTLAB:
                            mem = tlab->allocate(node_size, memtable::SkipListNode::REQUIRED_ALIGNMENT);
                            break;
                    }

                    if (sample) {
                        result.latency_samples_ns.push_back(
                            std::chrono::duration<double, std::nano>(Clock::now() - t_start).count());
                    }

                    if (mem == nullptr) {
                        result.allocation_failed = true;
                        break; // flag will be checked by main thread
                    }

                    auto& node = memtable::SkipListNode::construct(mem,
                                                                   key,
                                                                   val,
                                                                   static_cast<std::uint64_t>(op),
                                                                   memtable::ValueType::TypeValue,
                                                                   profile.height);
                    benchmark::DoNotOptimize(node);

                    if (mode == AllocationMode::BaselineNew) {
                        result.owned_allocations.push_back(mem);
                    }
                }

                // Signal main that this iteration's work is done.
                done_barrier.arrive_and_wait();
            }
        });
    }

    // -----------------------------------------------------------------------
    // Benchmark iteration loop — threads are already alive.
    // -----------------------------------------------------------------------
    std::vector<double> all_samples;
    std::size_t total_bytes = 0;

    for (auto _ : state) {
        (void)_;

        // ------ PAUSED: set up arena for this iteration --------------------
        state.PauseTiming();

        std::unique_ptr<memory::Arena> arena;
        if (mode != AllocationMode::BaselineNew) {
            const std::size_t total_ops = per_thread_ops * tc;
            const std::size_t capacity = (total_ops * node_size) + (tc * config::MemoryConfig::DEFAULT_TLAB_SIZE);

            auto exp = memory::Arena::create(make_arena_config(capacity));
            if (!exp.has_value()) {
                state.SkipWithError("Arena::create failed during benchmark setup");
                break;
            }
            arena = std::make_unique<memory::Arena>(std::move(exp.value()));
        }

        shared_arena.store(arena.get(), std::memory_order_release);

        state.ResumeTiming();
        // ------ TIMING: only allocation work happens here ------------------

        start_barrier.arrive_and_wait(); // wake all workers
        done_barrier.arrive_and_wait();  // wait for all workers to finish

        // ------ PAUSED: collect stats, free baseline_new memory ------------
        state.PauseTiming();

        for (const auto& result : results) {
            if (result.allocation_failed) {
                state.SkipWithError("Allocation failed inside worker thread");
                goto teardown;
            }
        }

        total_bytes += per_thread_ops * tc * node_size;

        for (auto& result : results) {
            all_samples.insert(all_samples.end(), result.latency_samples_ns.begin(), result.latency_samples_ns.end());
        }

        cleanup_owned_allocations(results);

        state.ResumeTiming();
    }

teardown:
    // Signal threads to exit, then join.
    stop_flag.store(true, std::memory_order_release);
    start_barrier.arrive_and_wait();
    for (auto& w : workers)
        w.join();

    const auto total_ops = static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(thread_count)
                           * static_cast<std::int64_t>(per_thread_ops);

    const auto latency = summarize_latencies(std::move(all_samples));

    state.SetItemsProcessed(total_ops);
    state.SetBytesProcessed(static_cast<std::int64_t>(total_bytes));
    state.counters["p50_ns"] = latency.p50_ns;
    state.counters["p99_ns"] = latency.p99_ns;
    state.counters["alloc_size_bytes"] = static_cast<double>(profile.allocation_size());
}

static void BM_NewAllocate(benchmark::State& state) {
    run_benchmark(state, AllocationMode::BaselineNew, "baseline_new");
}
static void BM_ArenaAllocate(benchmark::State& state) {
    run_benchmark(state, AllocationMode::ArenaOnly, "arena_only");
}
static void BM_TLABAllocate(benchmark::State& state) {
    run_benchmark(state, AllocationMode::ArenaWithTLAB, "arena_tlab");
}

void apply_allocator_args(benchmark::Benchmark* b) {
    for (int threads : {1, 2, 4, 8, 16}) {
        for (std::size_t i = 0; i < kNodeProfiles.size(); ++i) {
            b->Args({threads, static_cast<long long>(i)});
        }
    }
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_NewAllocate)->Apply(stratadb::bench::apply_allocator_args)->UseRealTime();
BENCHMARK(stratadb::bench::BM_ArenaAllocate)->Apply(stratadb::bench::apply_allocator_args)->UseRealTime();
BENCHMARK(stratadb::bench::BM_TLABAllocate)->Apply(stratadb::bench::apply_allocator_args)->UseRealTime();