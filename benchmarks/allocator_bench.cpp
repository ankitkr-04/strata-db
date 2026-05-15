// benchmarks/allocator_bench.cpp
//
// StrataDB Allocator Microbenchmarks
// Measures pure throughput of raw memory allocation + SkipListNode initialization.

#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"

#include <barrier>
#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <thread>
#include <vector>

namespace stratadb::bench {
namespace {

constexpr std::size_t kPerThreadTargetBytes = 4ULL * 1024ULL * 1024ULL;
constexpr std::size_t kMinimumOpsPerThread = 4096;

enum class AllocationMode : std::uint8_t {
    BaselineNew,
    ArenaOnly,
    ArenaWithTLAB,
};

// Padded and aligned to ensure strict cache-line isolation.
// Written to ONLY outside the timed hot-path to guarantee zero false sharing.
struct alignas(utils::CACHE_LINE_SIZE) WorkerStats {
    std::vector<void*> allocs;
    std::size_t ops_completed{0};
    bool failed{false};

    // Padding to ensure size is exactly a multiple of 64 bytes
    char _pad[utils::CACHE_LINE_SIZE - (sizeof(std::vector<void*>) + sizeof(std::size_t) + sizeof(bool))] = {0};
};

static_assert(sizeof(WorkerStats) % utils::CACHE_LINE_SIZE == 0, "WorkerStats risks false sharing.");

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
template <AllocationMode Mode>
void run_benchmark(benchmark::State& state) {
    const int thread_count = static_cast<int>(state.range(0));
    const auto& profile = profile_from_index(static_cast<int>(state.range(1)));
    const std::size_t per_thread_ops = ops_per_thread(profile);
    const std::size_t node_size = profile.allocation_size();
    const auto tc = static_cast<std::size_t>(thread_count);

    std::vector<std::string> thread_keys(tc);
    std::vector<std::string> thread_values(tc);
    for (std::size_t t = 0; t < tc; ++t) {
        thread_keys[t] = make_payload(profile.key_bytes, static_cast<char>('a' + (t % 26)));
        thread_values[t] = make_payload(profile.value_bytes, static_cast<char>('k' + (t % 10)));
    }

    // Barrier provides happens-before guarantee; no atomic needed.
    memory::Arena* shared_arena = nullptr;
    std::vector<WorkerStats> w_stats(tc);

    // Resize (not reserve) to allow branchless `allocs[op] = mem` assignment in the hot loop.
    for (std::size_t t = 0; t < tc; ++t) {
        w_stats[t].allocs.resize(per_thread_ops, nullptr);
    }

    bool stop_flag = false;

    // The 3-Barrier Sandwich
    std::barrier<> setup_barrier(thread_count + 1);
    std::barrier<> go_barrier(thread_count + 1);
    std::barrier<> done_barrier(thread_count + 1);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int ti = 0; ti < thread_count; ++ti) {
        workers.emplace_back([&, t = static_cast<std::size_t>(ti)]() {
            const std::string& key = thread_keys[t];
            const std::string& val = thread_values[t];

            while (true) {
                // Phase 1: Setup & Sync
                setup_barrier.arrive_and_wait();

                if (stop_flag) {
                    break;
                }

                if constexpr (Mode != AllocationMode::BaselineNew) {
                    if (!shared_arena) {
                        w_stats[t].failed = true;
                        go_barrier.arrive_and_wait();
                        done_barrier.arrive_and_wait();
                        continue;
                    }
                }

                // Registers for the hot loop (avoids cache line writes)
                std::size_t local_ops = 0;
                bool local_failed = false;
                void** local_allocs = w_stats[t].allocs.data();

                // Park precisely at the timed window edge
                go_barrier.arrive_and_wait();

                // Pure Hot Path
                // Every mode does exactly the same pointer tracking and compiler fencing.
                if constexpr (Mode == AllocationMode::ArenaWithTLAB) {
                    memory::TLAB tlab(*shared_arena);
                    for (std::size_t op = 0; op < per_thread_ops; ++op) {
                        void* mem = tlab.allocate(node_size, memtable::SkipListNode::REQUIRED_ALIGNMENT);
                        if (!mem) {
                            local_failed = true;
                            break;
                        }

                        local_allocs[op] = mem;
                        benchmark::DoNotOptimize(mem);

                        auto& node = memtable::SkipListNode::construct(mem,
                                                                       key,
                                                                       val,
                                                                       static_cast<std::uint64_t>(op),
                                                                       memtable::ValueType::TypeValue,
                                                                       profile.height);
                        benchmark::DoNotOptimize(node);
                        benchmark::ClobberMemory();
                        local_ops++;
                    }
                } else if constexpr (Mode == AllocationMode::ArenaOnly) {
                    for (std::size_t op = 0; op < per_thread_ops; ++op) {
                        void* mem =
                            shared_arena->allocate_aligned(node_size, memtable::SkipListNode::REQUIRED_ALIGNMENT);
                        if (!mem) {
                            local_failed = true;
                            break;
                        }

                        local_allocs[op] = mem;
                        benchmark::DoNotOptimize(mem);

                        auto& node = memtable::SkipListNode::construct(mem,
                                                                       key,
                                                                       val,
                                                                       static_cast<std::uint64_t>(op),
                                                                       memtable::ValueType::TypeValue,
                                                                       profile.height);
                        benchmark::DoNotOptimize(node);
                        benchmark::ClobberMemory();
                        local_ops++;
                    }
                } else if constexpr (Mode == AllocationMode::BaselineNew) {
                    for (std::size_t op = 0; op < per_thread_ops; ++op) {
                        void* mem = ::operator new(node_size,
                                                   std::align_val_t(memtable::SkipListNode::REQUIRED_ALIGNMENT),
                                                   std::nothrow);
                        if (!mem) {
                            local_failed = true;
                            break;
                        }

                        local_allocs[op] = mem;
                        benchmark::DoNotOptimize(mem);

                        auto& node = memtable::SkipListNode::construct(mem,
                                                                       key,
                                                                       val,
                                                                       static_cast<std::uint64_t>(op),
                                                                       memtable::ValueType::TypeValue,
                                                                       profile.height);
                        benchmark::DoNotOptimize(node);
                        benchmark::ClobberMemory();
                        local_ops++;
                    }
                }

                // Write back to memory exactly once
                w_stats[t].ops_completed = local_ops;
                w_stats[t].failed = local_failed;

                done_barrier.arrive_and_wait();
            }
        });
    }

    std::size_t total_bytes = 0;
    std::size_t total_ops_completed = 0;

    for (auto _ : state) {
        state.PauseTiming();

        std::unique_ptr<memory::Arena> arena;
        if constexpr (Mode != AllocationMode::BaselineNew) {
            // Tight calculated capacity + 16MB safety headroom
            const std::size_t capacity =
                (per_thread_ops * tc * node_size) + (tc * config::MemoryConfig::DEFAULT_TLAB_SIZE) + (16ULL << 20);
            auto exp = memory::Arena::create(make_arena_config(capacity));
            if (!exp.has_value()) {
                state.SkipWithError("Arena::create failed during benchmark setup");
                state.ResumeTiming();
                break;
            }
            arena = std::make_unique<memory::Arena>(std::move(exp.value()));
        }

        shared_arena = arena.get(); // Visible due to barrier

        setup_barrier.arrive_and_wait();

        // Timing covers thread dispatch + hot path + join barrier
        state.ResumeTiming();
        go_barrier.arrive_and_wait();
        done_barrier.arrive_and_wait();
        state.PauseTiming();

        bool has_failure = false;
        for (std::size_t t = 0; t < tc; ++t) {
            if (w_stats[t].failed)
                has_failure = true;
            total_ops_completed += w_stats[t].ops_completed;
            total_bytes += w_stats[t].ops_completed * node_size;

            // Clean up baseline memory safely outside the timed window
            if constexpr (Mode == AllocationMode::BaselineNew) {
                for (std::size_t i = 0; i < w_stats[t].ops_completed; ++i) {
                    ::operator delete(w_stats[t].allocs[i],
                                      std::align_val_t(memtable::SkipListNode::REQUIRED_ALIGNMENT));
                }
            }
        }

        state.ResumeTiming();
        if (has_failure) {
            state.SkipWithError("Allocation failed inside worker thread");
            break;
        }
    }

    state.PauseTiming();
    stop_flag = true;
    setup_barrier.arrive_and_wait(); // Final release for clean exit

    for (auto& w : workers) {
        w.join();
    }

    state.SetItemsProcessed(static_cast<std::int64_t>(total_ops_completed));
    state.SetBytesProcessed(static_cast<std::int64_t>(total_bytes));
    state.counters["alloc_size_bytes"] = static_cast<double>(profile.allocation_size());
}

static void BM_BaselineAllocInit(benchmark::State& state) {
    run_benchmark<AllocationMode::BaselineNew>(state);
}
static void BM_ArenaAllocInit(benchmark::State& state) {
    run_benchmark<AllocationMode::ArenaOnly>(state);
}
static void BM_TLABAllocInit(benchmark::State& state) {
    run_benchmark<AllocationMode::ArenaWithTLAB>(state);
}

void apply_allocator_args(benchmark::Benchmark* b) {
    for_each_supported_thread_count([&](int threads) {
        for (std::size_t i = 0; i < kNodeProfiles.size(); ++i) {
            b->Args({threads, static_cast<long long>(i)});
        }
    });
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_BaselineAllocInit)->Apply(stratadb::bench::apply_allocator_args)->UseRealTime();
BENCHMARK(stratadb::bench::BM_ArenaAllocInit)->Apply(stratadb::bench::apply_allocator_args)->UseRealTime();
BENCHMARK(stratadb::bench::BM_TLABAllocInit)->Apply(stratadb::bench::apply_allocator_args)->UseRealTime();