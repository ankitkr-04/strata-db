// benchmarks/allocator_lifecycle_bench.cpp
//
// Full MemTable lifecycle proof:
//   - baseline constructs N SkipListNode allocations, then times the O(N) delete loop
//   - StrataDB constructs N nodes through TLAB, then times the O(1) Arena::reset()

#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <string>
#include <vector>

namespace stratadb::bench {
namespace {

constexpr std::size_t kNodeCount = 1'000'000;
constexpr std::size_t kDefaultHeadroom = 64ULL << 20; // 64 MiB
constexpr int kLifecycleProfileIndex = 1;             // node_256b

[[nodiscard]] auto lifecycle_profile() noexcept -> const NodeProfile& {
    return profile_from_index(kLifecycleProfileIndex);
}

void cleanup_baseline_nodes(std::vector<void*>& nodes, std::size_t count) noexcept {
    for (std::size_t i = 0; i < count; ++i) {
        ::operator delete(nodes[i], std::align_val_t(memtable::SkipListNode::REQUIRED_ALIGNMENT));
    }
}

static void BM_SystemDeleteLifecycle(benchmark::State& state) {
    const auto& profile = lifecycle_profile();
    const std::size_t node_size = profile.allocation_size();
    const std::string key = make_payload(profile.key_bytes, 'k');
    const std::string value = make_payload(profile.value_bytes, 'v');

    std::size_t total_nodes = 0;
    for (auto _ : state) {
        state.PauseTiming();

        std::vector<void*> nodes(kNodeCount, nullptr);
        std::size_t constructed = 0;
        bool failed = false;

        for (; constructed < kNodeCount; ++constructed) {
            void* mem =
                ::operator new(node_size, std::align_val_t(memtable::SkipListNode::REQUIRED_ALIGNMENT), std::nothrow);
            if (!mem) {
                failed = true;
                break;
            }

            nodes[constructed] = mem;
            auto& node = memtable::SkipListNode::construct(mem,
                                                           key,
                                                           value,
                                                           static_cast<std::uint64_t>(constructed),
                                                           memtable::ValueType::TypeValue,
                                                           profile.height);
            benchmark::DoNotOptimize(node);
        }

        if (failed) {
            cleanup_baseline_nodes(nodes, constructed);
            state.SkipWithError("operator new failed during lifecycle setup");
            state.ResumeTiming();
            break;
        }

        state.ResumeTiming();
        cleanup_baseline_nodes(nodes, constructed);
        benchmark::ClobberMemory();
        state.PauseTiming();

        total_nodes += constructed;
        state.ResumeTiming();
    }

    state.SetItemsProcessed(static_cast<std::int64_t>(total_nodes));
    state.counters["nodes_per_lifecycle"] = static_cast<double>(kNodeCount);
    state.counters["alloc_size_bytes"] = static_cast<double>(node_size);
}

static void BM_ArenaResetLifecycle(benchmark::State& state) {
    const auto& profile = lifecycle_profile();
    const std::size_t node_size = profile.allocation_size();
    const std::size_t capacity = (kNodeCount * (node_size + memtable::SkipListNode::REQUIRED_ALIGNMENT))
                                 + config::MemoryConfig::DEFAULT_TLAB_SIZE + effective_headroom(kDefaultHeadroom);
    const std::string key = make_payload(profile.key_bytes, 'k');
    const std::string value = make_payload(profile.value_bytes, 'v');

    std::size_t total_nodes = 0;
    for (auto _ : state) {
        state.PauseTiming();

        auto exp = memory::Arena::create(make_memory_cfg(capacity));
        if (!exp.has_value()) {
            state.SkipWithError("Arena::create failed during lifecycle setup");
            state.ResumeTiming();
            break;
        }

        auto arena = std::make_unique<memory::Arena>(std::move(exp.value()));
        {
            memory::TLAB tlab(*arena);
            bool failed = false;
            for (std::size_t i = 0; i < kNodeCount; ++i) {
                void* mem = tlab.allocate(node_size, memtable::SkipListNode::REQUIRED_ALIGNMENT);
                if (!mem) {
                    failed = true;
                    break;
                }

                auto& node = memtable::SkipListNode::construct(mem,
                                                               key,
                                                               value,
                                                               static_cast<std::uint64_t>(i),
                                                               memtable::ValueType::TypeValue,
                                                               profile.height);
                benchmark::DoNotOptimize(node);
            }

            if (failed) {
                state.SkipWithError("TLAB allocation failed during lifecycle setup");
                state.ResumeTiming();
                break;
            }
        }

        state.ResumeTiming();
        arena->reset();
        benchmark::ClobberMemory();
        state.PauseTiming();

        total_nodes += kNodeCount;
        state.ResumeTiming();
    }

    state.SetItemsProcessed(static_cast<std::int64_t>(total_nodes));
    state.counters["nodes_per_lifecycle"] = static_cast<double>(kNodeCount);
    state.counters["alloc_size_bytes"] = static_cast<double>(node_size);
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_SystemDeleteLifecycle)->UseRealTime();
BENCHMARK(stratadb::bench::BM_ArenaResetLifecycle)->UseRealTime();
