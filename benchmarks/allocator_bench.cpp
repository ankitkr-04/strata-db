#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"

#include <atomic>
#include <benchmark/benchmark.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
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

void run_allocator_iteration(AllocationMode mode,
                             const NodeProfile& profile,
                             int thread_count,
                             std::size_t per_thread_ops,
                             std::vector<WorkerResult>& results,
                             std::size_t& bytes_touched,
                             memory::Arena* arena) {
    const std::size_t node_size = profile.allocation_size();
    const std::size_t total_ops = per_thread_ops * static_cast<std::size_t>(thread_count);
    bytes_touched = total_ops * node_size;

    OneShotStartGate gate(thread_count);
    std::vector<std::thread> workers;
    workers.reserve(static_cast<std::size_t>(thread_count));

    for (int thread_index = 0; thread_index < thread_count; ++thread_index) {
        workers.emplace_back([&, thread_index]() {
            auto& result = results[static_cast<std::size_t>(thread_index)];
            result.owned_allocations.reserve(per_thread_ops);
            result.latency_samples_ns.reserve((per_thread_ops / (kLatencySampleMask + 1)) + 1);

            const std::string key = make_payload(profile.key_bytes, static_cast<char>('a' + (thread_index % 26)));
            const std::string value = make_payload(profile.value_bytes, static_cast<char>('k' + (thread_index % 10)));

            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
            memory::TLAB tlab = arena ? memory::TLAB(*arena) : memory::TLAB(*reinterpret_cast<memory::Arena*>(0));
            gate.arrive_and_wait();

            for (std::size_t op = 0; op < per_thread_ops; ++op) {
                void* mem = nullptr;
                const bool sample = (op & kLatencySampleMask) == 0;
                const auto start = sample ? Clock::now() : Clock::time_point{};

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
                        mem = tlab.allocate(node_size, memtable::SkipListNode::REQUIRED_ALIGNMENT);
                        break;
                }

                if (sample) {
                    const auto stop = Clock::now();
                    const auto latency = std::chrono::duration<double, std::nano>(stop - start).count();
                    result.latency_samples_ns.push_back(latency);
                }

                if (mem == nullptr) {
                    throw std::runtime_error("Allocation failed during allocator benchmark");
                }

                auto& node = memtable::SkipListNode::construct(mem,
                                                               key,
                                                               value,
                                                               static_cast<std::uint64_t>(op),
                                                               memtable::ValueType::TypeValue,
                                                               profile.height);
                benchmark::DoNotOptimize(node);

                if (mode == AllocationMode::BaselineNew) {
                    result.owned_allocations.push_back(mem);
                }
            }
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }
}

void cleanup_owned_allocations(std::vector<WorkerResult>& results) noexcept {
    for (auto& result : results) {
        for (void* ptr : result.owned_allocations) {
            ::operator delete(ptr, std::align_val_t(memtable::SkipListNode::REQUIRED_ALIGNMENT));
        }
        result.owned_allocations.clear();
    }
}

void run_benchmark(benchmark::State& state, AllocationMode mode, std::string_view mode_name) {
    const int thread_count = static_cast<int>(state.range(0));
    const auto& profile = profile_from_index(static_cast<int>(state.range(1)));
    const std::size_t per_thread_ops = ops_per_thread(profile);

    std::vector<double> all_samples;
    std::size_t total_bytes = 0;

    state.SetLabel(std::string(mode_name) + "/" + profile.name);

    for (auto _ : state) {
        (void)_;

        state.PauseTiming();

        std::vector<WorkerResult> results(static_cast<std::size_t>(thread_count));

        std::size_t iteration_bytes = 0;

        std::unique_ptr<memory::Arena> arena;

        if (mode != AllocationMode::BaselineNew) {
            const std::size_t node_size = profile.allocation_size();
            const std::size_t total_ops = per_thread_ops * static_cast<std::size_t>(thread_count);

            iteration_bytes = total_ops * node_size;

            const std::size_t arena_capacity =
                iteration_bytes + (static_cast<std::size_t>(thread_count) * config::MemoryConfig::DEFAULT_TLAB_SIZE);

            auto arena_exp = memory::Arena::create(make_arena_config(arena_capacity));

            if (!arena_exp.has_value()) {
                throw std::runtime_error("Arena::create failed during benchmark setup");
            }

            arena = std::make_unique<memory::Arena>(std::move(arena_exp.value()));
        }

        state.ResumeTiming();

        run_allocator_iteration(mode, profile, thread_count, per_thread_ops, results, iteration_bytes, arena.get());

        state.PauseTiming();

        total_bytes += iteration_bytes;

        for (auto& result : results) {
            all_samples.insert(all_samples.end(), result.latency_samples_ns.begin(), result.latency_samples_ns.end());
        }

        cleanup_owned_allocations(results);

        state.ResumeTiming();
    }

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

void apply_allocator_args(benchmark::Benchmark* benchmark_handle) {
    for (int threads : {1, 2, 4, 8, 16}) {
        for (std::size_t profile_index = 0; profile_index < kNodeProfiles.size(); ++profile_index) {
            benchmark_handle->Args({threads, static_cast<long long>(profile_index)});
        }
    }
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_NewAllocate)->Apply(stratadb::bench::apply_allocator_args);
BENCHMARK(stratadb::bench::BM_ArenaAllocate)->Apply(stratadb::bench::apply_allocator_args);
BENCHMARK(stratadb::bench::BM_TLABAllocate)->Apply(stratadb::bench::apply_allocator_args);
