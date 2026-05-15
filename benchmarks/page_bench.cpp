// benchmarks/page_bench.cpp
//
// StrataDB Page Strategy & Prefaulting Benchmarks
// Measures the OS-level paging impact (Standard 4K vs Huge 2M) and prefaulting costs.

#include "benchmark_common.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/memtable/memtable_result.hpp"
#include "stratadb/memtable/skiplist_memtable.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <string>

namespace stratadb::bench {
namespace {

constexpr std::size_t kScanNodeCount = 524288;
constexpr std::size_t kPrefaultArenaBytes = 2048ULL * 1024ULL * 1024ULL; // 2 GB
constexpr std::size_t kFirstWriteOps = 32768;

struct ScanFixture {
    std::unique_ptr<memory::Arena> arena;
    std::unique_ptr<memtable::SkipListMemTable> memtable;
    std::size_t inserted{0};
    std::size_t exact_payload_bytes{0};
};

[[nodiscard]] auto build_scan_fixture(config::PageStrategy page_strategy) -> std::expected<ScanFixture, std::string> {
    const auto& profile = profile_from_index(1);
    const std::size_t node_bytes = profile.allocation_size();

    // Leverage unified dynamic headroom
    const std::size_t arena_bytes = (kScanNodeCount * node_bytes) + effective_headroom(64ULL * 1024ULL * 1024ULL);

    // Call unified make_memory_cfg from benchmark_common.hpp
    auto arena_exp = memory::Arena::create(make_memory_cfg(arena_bytes, page_strategy, false));
    if (!arena_exp.has_value()) {
        return std::unexpected("Arena::create failed (Environment may lack Huge Page support)");
    }

    ScanFixture fixture;
    fixture.arena = std::make_unique<memory::Arena>(std::move(arena_exp.value()));
    // Call unified make_memtable_cfg from benchmark_common.hpp
    fixture.memtable = std::make_unique<memtable::SkipListMemTable>(*fixture.arena, make_memtable_cfg(arena_bytes));

    memory::TLAB tlab(*fixture.arena);
    const std::string value = make_payload(profile.value_bytes, 'v');

    for (std::size_t i = 0; i < kScanNodeCount; ++i) {
        const std::string key = make_ordered_key(static_cast<std::uint64_t>(i), profile.key_bytes);
        const auto result = fixture.memtable->put(key, value, tlab);
        if (result != memtable::PutResult::Ok) {
            return std::unexpected("Memtable population stopped before reaching scan target");
        }
        ++fixture.inserted;
    }

    // Measure literal bytes touched (excluding internal structural node bytes)
    fixture.exact_payload_bytes = fixture.inserted * (profile.key_bytes + profile.value_bytes);
    return fixture;
}

[[nodiscard]] auto scan_memtable_once(const memtable::SkipListMemTable& table) noexcept -> std::size_t {
    std::size_t checksum = 0;
    table.scan([&checksum](const memtable::SkipListMemTable::EntryView& entry) {
        // Strong Consumer: Force the CPU to fetch and touch every single byte of payload,
        // accurately measuring real memory bandwidth and TLB pressure.
        for (char b : entry.key) {
            checksum += static_cast<std::size_t>(static_cast<unsigned char>(b));
        }
        for (char b : entry.value) {
            checksum += static_cast<std::size_t>(static_cast<unsigned char>(b));
        }
    });
    return checksum;
}

// BM_SkipListScanWarmed
// Measures warmed-cache sequential scan traversal.
static void BM_SkipListScanWarmed(benchmark::State& state) {
    const bool use_huge_pages = state.range(0) != 0;

    auto page_strategy = use_huge_pages ? config::PageStrategy::Huge2M_Strict : config::PageStrategy::Standard4K;

    // Honor global page strategy override from env variables if provided
    if (const auto& override_ps = global_bench_config().page_strategy) {
        page_strategy = *override_ps;
    }

    auto fixture_exp = build_scan_fixture(page_strategy);
    if (!fixture_exp.has_value()) {
        state.SkipWithError(fixture_exp.error().c_str());
        return;
    }

    auto fixture = std::move(fixture_exp.value());
    state.SetLabel(use_huge_pages ? "huge_2m_strict" : "standard_4k");

    for (auto _ : state) {
        (void)_;
        // Caches are slightly warm from the build_scan_fixture step
        auto checksum = scan_memtable_once(*fixture.memtable);
        benchmark::DoNotOptimize(checksum);
    }

    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations())
                            * static_cast<std::int64_t>(fixture.inserted));
    state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations())
                            * static_cast<std::int64_t>(fixture.exact_payload_bytes));
}

// BM_ArenaCreationPrefault
// Measures the OS latency of mapping vs. strictly prefaulting memory pages.
static void BM_ArenaCreationPrefault(benchmark::State& state) {
    const bool prefault = state.range(0) != 0;
    state.SetLabel(prefault ? "prefault_true" : "prefault_false");

    const std::size_t bytes = effective_headroom(kPrefaultArenaBytes);

    for (auto _ : state) {
        (void)_;
        auto arena_exp = memory::Arena::create(make_memory_cfg(bytes, config::PageStrategy::Standard4K, prefault));
        if (!arena_exp.has_value()) {
            state.SkipWithError("Arena::create failed during prefault startup benchmark");
            return;
        }
        auto arena = std::move(arena_exp.value());
        auto memory_used = arena.memory_used();
        benchmark::DoNotOptimize(memory_used);
    }
}

// BM_InitialBatchWritePrefault
// Measures the latency of a batch of fresh allocations (measuring page fault
// stalls if the arena wasn't prefaulted).
static void BM_InitialBatchWritePrefault(benchmark::State& state) {
    const bool prefault = state.range(0) != 0;
    const auto& profile = profile_from_index(1);
    const std::size_t node_size = profile.allocation_size();
    const std::string key = make_payload(profile.key_bytes, 'k');
    const std::string value = make_payload(profile.value_bytes, 'v');

    // Leverage dynamic operations and capacity boundaries
    const std::size_t ops = effective_ops(kFirstWriteOps);
    const std::size_t bytes = effective_headroom(kPrefaultArenaBytes);

    state.SetLabel(prefault ? "prefault_true" : "prefault_false");

    for (auto _ : state) {
        (void)_;
        state.PauseTiming();
        auto arena_exp = memory::Arena::create(make_memory_cfg(bytes, config::PageStrategy::Standard4K, prefault));
        if (!arena_exp.has_value()) {
            state.SkipWithError("Arena::create failed during initial-batch benchmark setup");
            return;
        }

        auto arena = std::move(arena_exp.value());
        memory::TLAB tlab(arena);
        state.ResumeTiming();

        for (std::size_t i = 0; i < ops; ++i) {
            void* mem = tlab.allocate(node_size, memtable::SkipListNode::REQUIRED_ALIGNMENT);
            if (mem == nullptr) {
                state.SkipWithError("TLAB allocation failed during initial-batch benchmark");
                return;
            }

            auto& node = memtable::SkipListNode::construct(mem,
                                                           key,
                                                           value,
                                                           static_cast<std::uint64_t>(i),
                                                           memtable::ValueType::TypeValue,
                                                           profile.height);
            benchmark::DoNotOptimize(node);
            benchmark::ClobberMemory();
        }

        state.PauseTiming();
        state.ResumeTiming(); // API compliance
    }

    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(ops));
    state.SetBytesProcessed(static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(ops * node_size));
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_SkipListScanWarmed)->Arg(0)->Arg(1);
BENCHMARK(stratadb::bench::BM_ArenaCreationPrefault)->Arg(0)->Arg(1);
BENCHMARK(stratadb::bench::BM_InitialBatchWritePrefault)->Arg(0)->Arg(1);