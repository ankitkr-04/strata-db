// benchmarks/memtable_bench.cpp
//
// Comprehensive StrataDB SkipListMemTable benchmark suite.
//
// Benchmark matrix:
//   BM_MemTablePut          — write throughput: (threads × profile × key_dist)
//   BM_MemTableGet          — read throughput:  (threads × profile × hit_pct)
//   BM_MemTableRemove       — tombstone throughput: (threads × profile)
//   BM_MemTableScan         — forward scan: (node_count) [DISABLED]
//   BM_MemTableMixedRW      — concurrent mixed: (threads × read_pct × profile)
//   BM_MemTablePutScaling   — thread-scaling curve: (threads), small_kv, random
//
// Every benchmark reports:
//   items/s  — operations per second
//   bytes/s  — payload bytes per second
//   p50_ns   — median per-op latency
//   p99_ns   — 99th-percentile per-op latency
//
// Run with:
//   ./build/bench/memtable_bench --benchmark_min_time=0.5s
//
// Flamegraph-ready run (requires STRATADB_ENABLE_PROFILING=ON):
//   perf stat -e cache-misses,dTLB-load-misses,cycles,instructions
//       ./build/bench/memtable_bench --benchmark_filter=BM_MemTableGet

#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/config/memtable_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/memtable/memtable_result.hpp"
#include "stratadb/memtable/skiplist_memtable.hpp"
#include "stratadb/utils/hardware.hpp"

#include <algorithm>
#include <atomic>
#include <barrier>
#include <benchmark/benchmark.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace stratadb::bench {
namespace {

using Clock = std::chrono::steady_clock;

// ═══════════════════════════════════════════════════════════════════
// Workload Profiles
// ═══════════════════════════════════════════════════════════════════

struct KVProfile {
    const char* name;
    std::size_t key_bytes;
    std::size_t value_bytes;
};

inline constexpr std::array<KVProfile, 4> kKVProfiles{{
    {.name = "tiny", .key_bytes = 8, .value_bytes = 32},      // counters / flags
    {.name = "small", .key_bytes = 24, .value_bytes = 128},   // session keys / web KV
    {.name = "medium", .key_bytes = 64, .value_bytes = 512},  // document fragments
    {.name = "large", .key_bytes = 128, .value_bytes = 2048}, // serialized protos / blobs
}};

[[nodiscard]] auto kv_profile(int idx) noexcept -> const KVProfile& {
    return kKVProfiles.at(static_cast<std::size_t>(idx));
}

// Key distribution (range arg encoding)
//   0 = Sequential  — monotonically increasing, best-case cache behavior
//   1 = Uniform     — iid uniform random over [0, key_space)
//   2 = Hotspot     — 80% of ops hit 20% of keys (Pareto-like hot set)
enum class KeyDist : uint8_t { Sequential = 0, Uniform = 1, Hotspot = 2 };

[[nodiscard]] auto dist_name(KeyDist d) noexcept -> const char* {
    switch (d) {
        case KeyDist::Sequential:
            return "seq";
        case KeyDist::Uniform:
            return "rnd";
        case KeyDist::Hotspot:
            return "hot";
    }
    return "?";
}

// ═══════════════════════════════════════════════════════════════════
// Key Generation
// ═══════════════════════════════════════════════════════════════════

// Pre-generate all keys for a workload to avoid measuring std::string
// construction inside the timed region.
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] auto pregenerate_keys(std::size_t count, std::size_t key_bytes, KeyDist dist, std::uint64_t thread_seed)
    -> std::vector<std::string> {
    std::mt19937_64 rng(thread_seed ^ 0xc0ffee'dead'beefULL);

    const std::size_t hot_boundary = std::max<std::size_t>(1, count / 5); // 20% = hot set

    std::vector<std::string> keys;
    keys.reserve(count);

    for (std::size_t i = 0; i < count; ++i) {
        std::size_t idx = i;

        switch (dist) {
            case KeyDist::Sequential:
                break; // idx unchanged

            case KeyDist::Uniform:
                idx = rng() % count;
                break;

            case KeyDist::Hotspot: {
                // 80 % of ops → bottom 20 % of key space
                const bool hot = (rng() % 100u) < 80u;
                idx = hot ? (rng() % hot_boundary)
                          : (hot_boundary + (rng() % std::max<std::size_t>(1, count - hot_boundary)));
                break;
            }
        }

        keys.push_back(make_ordered_key(idx, key_bytes));
    }

    return keys;
}

// ═══════════════════════════════════════════════════════════════════
// Fixture Helpers
// ═══════════════════════════════════════════════════════════════════

[[nodiscard]] auto make_memory_cfg(std::size_t bytes) noexcept -> config::MemoryConfig {
    config::MemoryConfig c;
    c.page_strategy = config::PageStrategy::Standard4K;
    c.prefault_on_init = false;
    c.total_budget_bytes = bytes;
    c.tlab_size_bytes = config::MemoryConfig::DEFAULT_TLAB_SIZE;
    c.block_alignment_bytes = config::MemoryConfig::DEFAULT_BLOCK_ALIGNMENT;
    return c;
}

[[nodiscard]] auto make_memtable_cfg(std::size_t budget) noexcept -> config::MemTableConfig {
    config::MemTableConfig c;
    // Set triggers above budget so we never stall/flush during benchmarks.
    c.max_size_bytes = budget;
    c.flush_trigger_bytes = std::numeric_limits<std::size_t>::max();
    c.stall_trigger_bytes = std::numeric_limits<std::size_t>::max();
    return c;
}

struct Fixture {
    std::unique_ptr<memory::Arena> arena;
    std::unique_ptr<memtable::SkipListMemTable> memtable;
};

[[nodiscard]] auto make_fixture(std::size_t arena_bytes) -> Fixture {
    auto exp = memory::Arena::create(make_memory_cfg(arena_bytes));
    if (!exp.has_value()) {
        std::terminate(); // benchmark cannot proceed without memory
    }
    Fixture f;
    f.arena = std::make_unique<memory::Arena>(std::move(*exp));
    f.memtable = std::make_unique<memtable::SkipListMemTable>(*f.arena, make_memtable_cfg(arena_bytes));
    return f;
}

[[nodiscard]] auto benchmark_arena_cap_bytes() noexcept -> std::size_t {
    static constexpr std::size_t kDefaultArenaCap = 256ULL << 20;
    const std::size_t host_ram = utils::total_physical_memory_bytes();
    if (host_ram == 0) {
        return kDefaultArenaCap;
    }

    const std::size_t scaled_cap = host_ram / 8;
    return std::min(kDefaultArenaCap, scaled_cap);
}

// Conservative node-size estimate: assume average height=6 (geometric p=0.25
// skip list), plus 25 % headroom for alignment waste and TLAB block overhead.
[[nodiscard]] auto node_budget(const KVProfile& p, std::size_t count) noexcept -> std::size_t {
    const std::size_t per_node = memtable::SkipListNode::allocation_size(6, p.key_bytes, p.value_bytes);
    return (per_node * count * 5) / 4; // ×1.25 headroom
}

// Extended latency summary: p50, p95, p99
struct LatencyExt {
    double p50_ns{0};
    double p95_ns{0};
    double p99_ns{0};
};

[[nodiscard]] auto summarize_ext(std::vector<double> samples) -> LatencyExt {
    if (samples.empty())
        return {};

    const auto pct = [&](double q) -> double {
        const auto idx = static_cast<std::size_t>(q * static_cast<double>(samples.size() - 1));
        std::nth_element(samples.begin(), samples.begin() + static_cast<std::ptrdiff_t>(idx), samples.end());
        return samples[idx];
    };

    // Call in ascending order so nth_element partitioning is valid
    const double p50 = pct(0.50);
    const double p95 = pct(0.95);
    const double p99 = pct(0.99);
    return {p50, p95, p99};
}

// ═══════════════════════════════════════════════════════════════════
// Constants
// ═══════════════════════════════════════════════════════════════════

// Reduced for architectural iteration:
static constexpr std::size_t kOpsPerThread = 4096;

// Reduced latency sampling frequency to cut down syscall overhead:
static constexpr std::size_t kLatMask = 1023; // 1 in 1024

// ═══════════════════════════════════════════════════════════════════
// BM_MemTablePut
// ═══════════════════════════════════════════════════════════════════

void run_put(benchmark::State& state) {
    const int nthreads = static_cast<int>(state.range(0));
    const auto& profile = kv_profile(static_cast<int>(state.range(1)));
    const auto dist = static_cast<KeyDist>(static_cast<int>(state.range(2)));
    const auto tc = static_cast<std::size_t>(nthreads);

    state.SetLabel(std::string("put/") + profile.name + "/" + dist_name(dist));

    // Pre-generate keys outside timed region; each thread gets a unique seed
    std::vector<std::vector<std::string>> t_keys(tc);
    std::vector<std::string> t_vals(tc);
    for (std::size_t t = 0; t < tc; ++t) {
        t_keys[t] = pregenerate_keys(kOpsPerThread, profile.key_bytes, dist, t * 0x1234'5678'abcd'ef01ULL);
        t_vals[t] = std::string(profile.value_bytes, static_cast<char>('A' + t % 26));
    }

    std::atomic<bool> stop{false};
    std::atomic<memory::Arena*> g_arena{nullptr};
    std::atomic<memtable::SkipListMemTable*> g_mt{nullptr};
    std::barrier<> go(nthreads + 1);
    std::barrier<> done(nthreads + 1);

    std::vector<bool> w_failed(tc, false);
    std::vector<std::vector<double>> w_lat(tc);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int ti = 0; ti < nthreads; ++ti) {
        // TSan FIX: Capture loop invariant pointers/references explicitly, capture `ti` via `t` by value.
        workers.emplace_back([&stop,
                              &g_arena,
                              &g_mt,
                              &go,
                              &done,
                              &w_failed,
                              &w_lat,
                              &t_keys,
                              &t_vals,
                              t = static_cast<std::size_t>(ti)] {
            const std::vector<std::string>& keys = t_keys[t];
            const std::string& val = t_vals[t];

            while (true) {
                go.arrive_and_wait();
                if (stop.load(std::memory_order_acquire))
                    break;

                memory::Arena* local_arena = g_arena.load(std::memory_order_acquire);
                auto* mt = g_mt.load(std::memory_order_acquire);

                if (!local_arena || !mt) {
                    w_failed[t] = true;
                    done.arrive_and_wait();
                    continue;
                }

                memory::TLAB tlab(*local_arena);

                w_lat[t].clear();
                w_lat[t].reserve(kOpsPerThread / (kLatMask + 1) + 1);
                w_failed[t] = false;

                for (std::size_t op = 0; op < kOpsPerThread; ++op) {
                    const bool sample = (op & kLatMask) == 0;
                    const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                    auto r = mt->put(keys[op], val, tlab);

                    if (sample) {
                        w_lat[t].push_back(std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
                    }

                    if (r == memtable::PutResult::OutOfMemory) {
                        w_failed[t] = true;
                        break;
                    }
                    benchmark::DoNotOptimize(r);
                }

                done.arrive_and_wait();
            }
        });
    }

    std::vector<double> all_lat;
    std::size_t payload_bytes = 0;

    for (auto _ : state) {
        (void)_;
        state.PauseTiming();

        const std::size_t cap =
            node_budget(profile, kOpsPerThread * tc) + tc * config::MemoryConfig::DEFAULT_TLAB_SIZE + (64ULL << 20);

        auto fix = make_fixture(cap);
        g_arena.store(fix.arena.get(), std::memory_order_release);
        g_mt.store(fix.memtable.get(), std::memory_order_release);

        state.ResumeTiming();
        go.arrive_and_wait();
        done.arrive_and_wait();
        state.PauseTiming();

        for (std::size_t t = 0; t < tc; ++t) {
            if (w_failed[t]) {
                state.SkipWithError("Arena OOM during put benchmark — increase headroom");
                goto teardown;
            }
        }

        payload_bytes += kOpsPerThread * tc * (profile.key_bytes + profile.value_bytes);
        for (std::size_t t = 0; t < tc; ++t) {
            all_lat.insert(all_lat.end(), w_lat[t].begin(), w_lat[t].end());
        }

        state.ResumeTiming();
    }

teardown:
    stop.store(true, std::memory_order_release);
    go.arrive_and_wait();
    for (auto& w : workers)
        w.join();

    const auto total_ops = static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(nthreads)
                           * static_cast<std::int64_t>(kOpsPerThread);

    const auto lat = summarize_ext(std::move(all_lat));
    state.SetItemsProcessed(total_ops);
    state.SetBytesProcessed(static_cast<std::int64_t>(payload_bytes));
    state.counters["p50_ns"] = lat.p50_ns;
    state.counters["p95_ns"] = lat.p95_ns;
    state.counters["p99_ns"] = lat.p99_ns;
}

static void BM_MemTablePut(benchmark::State& state) {
    run_put(state);
}

// ═══════════════════════════════════════════════════════════════════
// BM_MemTableGet
// ═══════════════════════════════════════════════════════════════════

void run_get(benchmark::State& state) {
    const int nthreads = static_cast<int>(state.range(0));
    const auto& profile = kv_profile(static_cast<int>(state.range(1)));
    const int hit_pct = static_cast<int>(state.range(2));
    const auto tc = static_cast<std::size_t>(nthreads);

    state.SetLabel("get/" + std::string(profile.name) + "/hit" + std::to_string(hit_pct));

    static constexpr std::size_t kPopulateCount = 131'072;

    const std::size_t cap = node_budget(profile, kPopulateCount) + (64ULL << 20);
    auto fix = make_fixture(cap);

    {
        memory::TLAB seed_tlab(*fix.arena);
        const std::string seed_val(profile.value_bytes, 'X');
        for (std::size_t i = 0; i < kPopulateCount; ++i) {
            const auto key = make_ordered_key(i, profile.key_bytes);
            if (fix.memtable->put(key, seed_val, seed_tlab) != memtable::PutResult::Ok) {
                state.SkipWithError("Fixture population failed — increase kPopulateCount budget");
                return;
            }
        }
    }

    std::mt19937_64 probe_rng(0xfeedface'cafe1234ULL);

    std::vector<std::vector<std::string>> t_probe_keys(tc);
    for (std::size_t t = 0; t < tc; ++t) {
        t_probe_keys[t].reserve(kOpsPerThread);
        for (std::size_t op = 0; op < kOpsPerThread; ++op) {
            const bool is_hit = static_cast<int>(probe_rng() % 100u) < hit_pct;
            const std::size_t idx =
                is_hit ? (probe_rng() % kPopulateCount) : (kPopulateCount + (probe_rng() % kPopulateCount));
            t_probe_keys[t].push_back(make_ordered_key(idx, profile.key_bytes));
        }
    }

    std::atomic<bool> stop{false};
    std::barrier<> go(nthreads + 1);
    std::barrier<> done(nthreads + 1);

    std::vector<std::vector<double>> w_lat(tc);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int ti = 0; ti < nthreads; ++ti) {
        // TSan FIX: Explicit explicit captures
        workers.emplace_back([&stop, &go, &done, &t_probe_keys, &fix, &w_lat, t = static_cast<std::size_t>(ti)] {
            const std::vector<std::string>& keys = t_probe_keys[t];
            const memtable::SkipListMemTable& mt = *fix.memtable;

            while (true) {
                go.arrive_and_wait();
                if (stop.load(std::memory_order_acquire))
                    break;

                w_lat[t].clear();
                w_lat[t].reserve(kOpsPerThread / (kLatMask + 1) + 1);

                for (std::size_t op = 0; op < kOpsPerThread; ++op) {
                    const bool sample = (op & kLatMask) == 0;
                    const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                    auto result = mt.get(keys[op]);

                    if (sample) {
                        w_lat[t].push_back(std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
                    }

                    benchmark::DoNotOptimize(result);
                }

                done.arrive_and_wait();
            }
        });
    }

    std::vector<double> all_lat;

    for (auto _ : state) {
        (void)_;
        go.arrive_and_wait();
        done.arrive_and_wait();

        state.PauseTiming();
        for (std::size_t t = 0; t < tc; ++t) {
            all_lat.insert(all_lat.end(), w_lat[t].begin(), w_lat[t].end());
        }
        state.ResumeTiming();
    }

    stop.store(true, std::memory_order_release);
    go.arrive_and_wait();
    for (auto& w : workers)
        w.join();

    const auto total_ops = static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(nthreads)
                           * static_cast<std::int64_t>(kOpsPerThread);

    const auto lat = summarize_ext(std::move(all_lat));
    state.SetItemsProcessed(total_ops);
    state.counters["p50_ns"] = lat.p50_ns;
    state.counters["p95_ns"] = lat.p95_ns;
    state.counters["p99_ns"] = lat.p99_ns;
    state.counters["hit_pct"] = static_cast<double>(hit_pct);
}

static void BM_MemTableGet(benchmark::State& state) {
    run_get(state);
}

// ═══════════════════════════════════════════════════════════════════
// BM_MemTableRemove
// ═══════════════════════════════════════════════════════════════════

void run_remove(benchmark::State& state) {
    const int nthreads = static_cast<int>(state.range(0));
    const auto& profile = kv_profile(static_cast<int>(state.range(1)));
    const auto tc = static_cast<std::size_t>(nthreads);

    state.SetLabel(std::string("remove/") + profile.name);

    std::vector<std::vector<std::string>> t_keys(tc);
    for (std::size_t t = 0; t < tc; ++t) {
        t_keys[t] = pregenerate_keys(kOpsPerThread, profile.key_bytes, KeyDist::Uniform, t * 0xdeadface'1234'5678ULL);
    }

    std::atomic<bool> stop{false};
    std::atomic<memory::Arena*> g_arena{nullptr};
    std::atomic<memtable::SkipListMemTable*> g_mt{nullptr};
    std::barrier<> go(nthreads + 1);
    std::barrier<> done(nthreads + 1);

    std::vector<bool> w_failed(tc, false);
    std::vector<std::vector<double>> w_lat(tc);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int ti = 0; ti < nthreads; ++ti) {
        // TSan FIX: Explicit captures
        workers.emplace_back(
            [&stop, &go, &done, &g_arena, &g_mt, &t_keys, &w_failed, &w_lat, t = static_cast<std::size_t>(ti)] {
                const std::vector<std::string>& keys = t_keys[t];

                while (true) {
                    go.arrive_and_wait();
                    if (stop.load(std::memory_order_acquire))
                        break;

                    memory::Arena* local_arena = g_arena.load(std::memory_order_acquire);
                    if (!local_arena)
                        continue;

                    memory::TLAB tlab(*local_arena);
                    auto* mt = g_mt.load(std::memory_order_acquire);

                    w_lat[t].clear();
                    w_lat[t].reserve(kOpsPerThread / (kLatMask + 1) + 1);
                    w_failed[t] = false;

                    for (std::size_t op = 0; op < kOpsPerThread; ++op) {
                        const bool sample = (op & kLatMask) == 0;
                        const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                        auto r = mt->remove(keys[op], tlab);

                        if (sample) {
                            w_lat[t].push_back(std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
                        }

                        if (r == memtable::PutResult::OutOfMemory) {
                            w_failed[t] = true;
                            break;
                        }
                        benchmark::DoNotOptimize(r);
                    }

                    done.arrive_and_wait();
                }
            });
    }

    std::vector<double> all_lat;

    for (auto _ : state) {
        (void)_;
        state.PauseTiming();

        const std::size_t tombstone_bytes = memtable::SkipListNode::allocation_size(1, profile.key_bytes, 0);
        const std::size_t cap =
            tombstone_bytes * kOpsPerThread * tc + tc * config::MemoryConfig::DEFAULT_TLAB_SIZE + (32ULL << 20);

        auto fix = make_fixture(cap);
        g_arena.store(fix.arena.get(), std::memory_order_release);
        g_mt.store(fix.memtable.get(), std::memory_order_release);

        state.ResumeTiming();
        go.arrive_and_wait();
        done.arrive_and_wait();
        state.PauseTiming();

        for (std::size_t t = 0; t < tc; ++t) {
            if (w_failed[t]) {
                state.SkipWithError("Arena OOM during remove benchmark");
                goto teardown;
            }
        }

        for (std::size_t t = 0; t < tc; ++t) {
            all_lat.insert(all_lat.end(), w_lat[t].begin(), w_lat[t].end());
        }

        state.ResumeTiming();
    }

teardown:
    stop.store(true, std::memory_order_release);
    go.arrive_and_wait();
    for (auto& w : workers)
        w.join();

    const auto total_ops = static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(nthreads)
                           * static_cast<std::int64_t>(kOpsPerThread);

    const auto lat = summarize_ext(std::move(all_lat));
    state.SetItemsProcessed(total_ops);
    state.counters["p50_ns"] = lat.p50_ns;
    state.counters["p95_ns"] = lat.p95_ns;
    state.counters["p99_ns"] = lat.p99_ns;
}

static void BM_MemTableRemove(benchmark::State& state) {
    run_remove(state);
}

// ═══════════════════════════════════════════════════════════════════
// BM_MemTableScan - DISABLED for rapid iteration
// ═══════════════════════════════════════════════════════════════════
/*
static void BM_MemTableScan(benchmark::State& state) {
    // Intentionally skipped to reduce bench time
}
*/

// ═══════════════════════════════════════════════════════════════════
// BM_MemTableMixedRW
// ═══════════════════════════════════════════════════════════════════

void run_mixed(benchmark::State& state) {
    const int nthreads = static_cast<int>(state.range(0));
    const int read_pct = static_cast<int>(state.range(1));
    const auto& profile = kv_profile(static_cast<int>(state.range(2)));
    const auto tc = static_cast<std::size_t>(nthreads);

    const int n_writers = std::max(1, (nthreads * (100 - read_pct)) / 100);

    state.SetLabel("mixed/" + std::string(profile.name) + "/R" + std::to_string(read_pct) + "W"
                   + std::to_string(100 - read_pct));

    auto fix = make_fixture(benchmark_arena_cap_bytes());

    static constexpr std::size_t kSeedKeys = 16'384;
    {
        memory::TLAB seed(*fix.arena);
        const std::string sv(profile.value_bytes, 'P');
        for (std::size_t i = 0; i < kSeedKeys; ++i) {
            (void)fix.memtable->put(make_ordered_key(i, profile.key_bytes), sv, seed);
        }
    }

    std::vector<std::atomic<std::size_t>> writer_cursors(static_cast<std::size_t>(n_writers));
    for (int w = 0; w < n_writers; ++w) {
        writer_cursors[static_cast<std::size_t>(w)].store(kSeedKeys
                                                              + static_cast<std::size_t>(w) * kOpsPerThread * 1000,
                                                          std::memory_order_relaxed);
    }

    std::atomic<std::size_t> total_inserted{kSeedKeys};
    std::string writer_val(profile.value_bytes, 'W');

    std::vector<std::size_t> w_put_count(tc, 0);
    std::vector<std::size_t> w_get_count(tc, 0);
    std::vector<std::size_t> w_oom_count(tc, 0);
    std::vector<std::vector<double>> w_lat(tc);

    std::atomic<bool> stop{false};
    std::barrier<> go(nthreads + 1);
    std::barrier<> done(nthreads + 1);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int ti = 0; ti < nthreads; ++ti) {
        // TSan FIX: Explicit captures
        workers.emplace_back([&stop,
                              &go,
                              &done,
                              &fix,
                              &writer_cursors,
                              &total_inserted,
                              &writer_val,
                              &profile,
                              &w_put_count,
                              &w_get_count,
                              &w_oom_count,
                              &w_lat,
                              n_writers,
                              t = static_cast<std::size_t>(ti)] {
            const bool is_writer = t < static_cast<std::size_t>(n_writers);
            const std::uint64_t rng_seed =
                static_cast<std::uint64_t>(t) * static_cast<std::uint64_t>(0xabcdef'123456ULL);
            std::mt19937_64 rng(rng_seed);

            while (true) {
                go.arrive_and_wait();
                if (stop.load(std::memory_order_acquire))
                    break;

                w_lat[t].clear();
                w_lat[t].reserve(kOpsPerThread / (kLatMask + 1) + 1);
                w_put_count[t] = 0;
                w_get_count[t] = 0;
                w_oom_count[t] = 0;

                if (is_writer) {
                    const std::size_t writer_idx = t;
                    memory::TLAB tlab(*fix.arena);

                    for (std::size_t op = 0; op < kOpsPerThread; ++op) {
                        const bool sample = (op & kLatMask) == 0;
                        const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                        const std::size_t idx = writer_cursors[writer_idx].fetch_add(1, std::memory_order_relaxed);
                        const auto key = make_ordered_key(idx, profile.key_bytes);
                        auto r = fix.memtable->put(key, writer_val, tlab);

                        if (sample) {
                            w_lat[t].push_back(std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
                        }

                        if (r == memtable::PutResult::OutOfMemory) {
                            ++w_oom_count[t];
                        } else {
                            total_inserted.fetch_add(1, std::memory_order_relaxed);
                            ++w_put_count[t];
                        }
                        benchmark::DoNotOptimize(r);
                    }

                } else {
                    for (std::size_t op = 0; op < kOpsPerThread; ++op) {
                        const bool sample = (op & kLatMask) == 0;
                        const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                        const std::size_t inserted = total_inserted.load(std::memory_order_acquire);
                        const std::size_t idx = rng() % std::max<std::size_t>(1, inserted);
                        const auto key = make_ordered_key(idx, profile.key_bytes);
                        auto result = fix.memtable->get(key);

                        if (sample) {
                            w_lat[t].push_back(std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
                        }

                        ++w_get_count[t];
                        benchmark::DoNotOptimize(result);
                    }
                }

                done.arrive_and_wait();
            }
        });
    }

    std::vector<double> all_lat;
    std::size_t sum_puts = 0;
    std::size_t sum_gets = 0;
    std::size_t sum_ooms = 0;

    for (auto _ : state) {
        (void)_;
        go.arrive_and_wait();
        done.arrive_and_wait();

        state.PauseTiming();
        for (std::size_t t = 0; t < tc; ++t) {
            sum_puts += w_put_count[t];
            sum_gets += w_get_count[t];
            sum_ooms += w_oom_count[t];
            all_lat.insert(all_lat.end(), w_lat[t].begin(), w_lat[t].end());
        }
        state.ResumeTiming();
    }

    stop.store(true, std::memory_order_release);
    go.arrive_and_wait();
    for (auto& w : workers)
        w.join();

    const auto total_ops = static_cast<std::int64_t>(sum_puts + sum_gets);
    const auto lat = summarize_ext(std::move(all_lat));

    state.SetItemsProcessed(total_ops);
    state.counters["put_ops"] = static_cast<double>(sum_puts);
    state.counters["get_ops"] = static_cast<double>(sum_gets);
    state.counters["oom_hits"] = static_cast<double>(sum_ooms);
    state.counters["p50_ns"] = lat.p50_ns;
    state.counters["p95_ns"] = lat.p95_ns;
    state.counters["p99_ns"] = lat.p99_ns;
}

static void BM_MemTableMixedRW(benchmark::State& state) {
    run_mixed(state);
}

// ═══════════════════════════════════════════════════════════════════
// BM_MemTablePutScaling
// ═══════════════════════════════════════════════════════════════════

static void BM_MemTablePutScaling(benchmark::State& state) {
    run_put(state);
}

// ═══════════════════════════════════════════════════════════════════
// BM_MemTablePutSteadyState - DISABLED for rapid iteration
// ═══════════════════════════════════════════════════════════════════
/*
void run_put_steady_state(benchmark::State& state) {
    // Intentionally skipped
}
static void BM_MemTablePutSteadyState(benchmark::State& state) {
    run_put_steady_state(state);
}
*/

// ═══════════════════════════════════════════════════════════════════
// Registration Helpers
// ═══════════════════════════════════════════════════════════════════

// Tightly constrained matrix to reduce benchmark explosion
void register_put_args(benchmark::Benchmark* b) {
    b->Args({1, 1, 1}); // 1 thread, small KV, uniform
    b->Args({4, 1, 1}); // 4 threads, small KV, uniform
}

void register_get_args(benchmark::Benchmark* b) {
    b->Args({1, 1, 100}); // 1 thread, small KV, 100% hit
    b->Args({4, 1, 100}); // 4 threads, small KV, 100% hit
}

void register_remove_args(benchmark::Benchmark* b) {
    b->Args({1, 1});
    b->Args({4, 1});
}

[[maybe_unused]] void register_scan_args([[maybe_unused]] benchmark::Benchmark* b) {
    // Disabled
}

void register_mixed_args(benchmark::Benchmark* b) {
    b->Args({4, 80, 1}); // 1 thread, 80% read, small KV
    b->Args({4, 80, 1}); // 4 threads, 80% read, small KV
}

void register_scaling_args(benchmark::Benchmark* b) {
    b->Args({1, 1, 1});
    b->Args({4, 1, 1});
}

} // namespace
} // namespace stratadb::bench

// ═══════════════════════════════════════════════════════════════════
// Benchmark Registration
// ═══════════════════════════════════════════════════════════════════

BENCHMARK(stratadb::bench::BM_MemTablePut)->Apply(stratadb::bench::register_put_args)->UseRealTime();

BENCHMARK(stratadb::bench::BM_MemTableGet)->Apply(stratadb::bench::register_get_args)->UseRealTime();

BENCHMARK(stratadb::bench::BM_MemTableRemove)->Apply(stratadb::bench::register_remove_args)->UseRealTime();

// BENCHMARK(stratadb::bench::BM_MemTableScan)->Apply(stratadb::bench::register_scan_args)->UseRealTime();

BENCHMARK(stratadb::bench::BM_MemTableMixedRW)
    ->Apply(stratadb::bench::register_mixed_args)
    ->UseRealTime()
    ->MeasureProcessCPUTime();

BENCHMARK(stratadb::bench::BM_MemTablePutScaling)->Apply(stratadb::bench::register_scaling_args)->UseRealTime();

// BENCHMARK(stratadb::bench::BM_MemTablePutSteadyState)->Apply(stratadb::bench::register_scaling_args)->UseRealTime();