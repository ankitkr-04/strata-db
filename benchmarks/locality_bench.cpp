// benchmarks/locality_bench.cpp
//
// Cache locality proof:
//   - baseline scans a shuffled pointer array of malloc-backed nodes
//   - StrataDB scans Arena-backed nodes in allocation order
//
// Pair this benchmark with:
//   ./scripts/bench.sh --target locality --perf --filter 'BM_.*Scan'

#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"

#include <algorithm>
#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

namespace stratadb::bench {
namespace {

constexpr std::size_t kNodeCount = 500'000;
constexpr std::size_t kPayloadStep = 64;
constexpr std::size_t kDefaultHeadroom = 64ULL << 20; // 64 MiB
constexpr int kLocalityProfileIndex = 3;              // node_1024b

[[nodiscard]] auto locality_profile() noexcept -> const NodeProfile& {
    return profile_from_index(kLocalityProfileIndex);
}

[[nodiscard]] auto touch_payload(const memtable::SkipListNode* node) noexcept -> std::uint64_t {
    std::uint64_t sum = 0;
    const std::string_view value = node->value();
    for (std::size_t i = 0; i < value.size(); i += kPayloadStep) {
        sum += static_cast<unsigned char>(value[i]);
    }
    return sum;
}

[[nodiscard]] auto scan_nodes(const std::vector<memtable::SkipListNode*>& nodes) noexcept -> std::uint64_t {
    std::uint64_t sum = 0;
    for (const auto* node : nodes) {
        benchmark::DoNotOptimize(node);
        sum += touch_payload(node);
    }
    benchmark::DoNotOptimize(sum);
    return sum;
}

void free_malloc_nodes(std::vector<memtable::SkipListNode*>& nodes) noexcept {
    for (auto* node : nodes) {
        ::operator delete(node, std::align_val_t(memtable::SkipListNode::REQUIRED_ALIGNMENT));
    }
    nodes.clear();
}

void free_fillers(std::vector<void*>& fillers) noexcept {
    for (void* ptr : fillers) {
        std::free(ptr);
    }
    fillers.clear();
}

[[nodiscard]] auto make_malloc_nodes() -> std::vector<memtable::SkipListNode*> {
    const auto& profile = locality_profile();
    const std::size_t node_size = profile.allocation_size();
    const std::string key = make_payload(profile.key_bytes, 'm');
    const std::string value = make_payload(profile.value_bytes, 'x');

    std::mt19937_64 rng(0x5157'adeb'0000'0001ULL);
    std::array<std::size_t, 4> filler_sizes{128, 256, 1024, 4096};
    std::vector<void*> fillers;
    fillers.reserve(kNodeCount / 8);

    std::vector<memtable::SkipListNode*> nodes;
    nodes.reserve(kNodeCount);

    for (std::size_t i = 0; i < kNodeCount; ++i) {
        // void* raw = std::malloc(node_size);
        void* raw =
            ::operator new(node_size, std::align_val_t(memtable::SkipListNode::REQUIRED_ALIGNMENT), std::nothrow);
        if (!raw) {
            free_malloc_nodes(nodes);
            free_fillers(fillers);
            return {};
        }

        auto& node = memtable::SkipListNode::construct(raw,
                                                       key,
                                                       value,
                                                       static_cast<std::uint64_t>(i),
                                                       memtable::ValueType::TypeValue,
                                                       profile.height);
        nodes.push_back(&node);

        if ((i & 7ULL) == 0) {
            const std::size_t filler_size = filler_sizes[static_cast<std::size_t>(rng() % filler_sizes.size())];
            void* filler = std::malloc(filler_size);
            if (filler) {
                std::memset(filler, static_cast<int>(i & 0xffULL), filler_size);
                fillers.push_back(filler);
            }
        }
    }

    for (std::size_t i = 0; i < fillers.size(); i += 2) {
        std::free(fillers[i]);
        fillers[i] = nullptr;
    }
    fillers.erase(std::remove(fillers.begin(), fillers.end(), nullptr), fillers.end());

    std::shuffle(nodes.begin(), nodes.end(), rng);
    free_fillers(fillers);
    return nodes;
}

struct ArenaFixture {
    std::unique_ptr<memory::Arena> arena;
    std::vector<memtable::SkipListNode*> nodes;
};

[[nodiscard]] auto make_arena_nodes() -> ArenaFixture {
    const auto& profile = locality_profile();
    const std::size_t node_size = profile.allocation_size();
    const std::size_t capacity = (kNodeCount * (node_size + memtable::SkipListNode::REQUIRED_ALIGNMENT))
                                 + config::MemoryConfig::DEFAULT_TLAB_SIZE + effective_headroom(kDefaultHeadroom);
    const std::string key = make_payload(profile.key_bytes, 'a');
    const std::string value = make_payload(profile.value_bytes, 'x');

    auto exp = memory::Arena::create(make_memory_cfg(capacity));
    if (!exp.has_value()) {
        return {};
    }

    ArenaFixture fixture;
    fixture.arena = std::make_unique<memory::Arena>(std::move(exp.value()));
    fixture.nodes.reserve(kNodeCount);

    memory::TLAB tlab(*fixture.arena);
    for (std::size_t i = 0; i < kNodeCount; ++i) {
        void* raw = tlab.allocate(node_size, memtable::SkipListNode::REQUIRED_ALIGNMENT);
        if (!raw) {
            return {};
        }

        auto& node = memtable::SkipListNode::construct(raw,
                                                       key,
                                                       value,
                                                       static_cast<std::uint64_t>(i),
                                                       memtable::ValueType::TypeValue,
                                                       profile.height);
        fixture.nodes.push_back(&node);
    }

    return fixture;
}

static void BM_FragmentedMallocScan(benchmark::State& state) {
    state.PauseTiming();
    auto nodes = make_malloc_nodes();
    if (nodes.size() != kNodeCount) {
        state.SkipWithError("malloc node fixture allocation failed");
        state.ResumeTiming();
        return;
    }
    state.ResumeTiming();

    std::uint64_t checksum = 0;
    for (auto _ : state) {
        checksum += scan_nodes(nodes);
    }

    state.PauseTiming();
    benchmark::DoNotOptimize(checksum);
    free_malloc_nodes(nodes);
    state.ResumeTiming();

    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(kNodeCount));
    state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(kNodeCount)
                            * static_cast<std::int64_t>(locality_profile().value_bytes));
}

static void BM_ArenaSequentialScan(benchmark::State& state) {
    state.PauseTiming();
    auto fixture = make_arena_nodes();
    if (!fixture.arena || fixture.nodes.size() != kNodeCount) {
        state.SkipWithError("Arena node fixture allocation failed");
        state.ResumeTiming();
        return;
    }
    state.ResumeTiming();

    std::uint64_t checksum = 0;
    for (auto _ : state) {
        checksum += scan_nodes(fixture.nodes);
    }

    benchmark::DoNotOptimize(checksum);
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(kNodeCount));
    state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(kNodeCount)
                            * static_cast<std::int64_t>(locality_profile().value_bytes));
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_FragmentedMallocScan)->UseRealTime();
BENCHMARK(stratadb::bench::BM_ArenaSequentialScan)->UseRealTime();
