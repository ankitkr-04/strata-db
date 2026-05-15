// benchmarks/memtable_bench.cpp
//
// StrataDB MemTable Microbenchmarks
// Full SkipListMemTable suite: put, get, remove, mixed R/W.

#include "benchmark_common.hpp"
#include "stratadb/config/memory_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/memtable/memtable_result.hpp"
#include "stratadb/memtable/skiplist_memtable.hpp"

#include <algorithm>
#include <barrier>
#include <benchmark/benchmark.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace stratadb::bench {
namespace {

using Clock = std::chrono::steady_clock;

// File-local default. Override via STRATADB_BENCH_OPS_PER_THREAD.
constexpr std::size_t kDefaultOpsPerThread = 4096;

// Default extra arena budget. Override via STRATADB_BENCH_ARENA_HEADROOM_MIB.
constexpr std::size_t kDefaultHeadroomLarge = 64ULL << 20; // 64 MiB  (put / mixed)
constexpr std::size_t kDefaultHeadroomSmall = 32ULL << 20; // 32 MiB  (remove)

struct KVProfile {
    const char* name;
    std::size_t key_bytes;
    std::size_t value_bytes;
};

inline constexpr std::array<KVProfile, 4> kKVProfiles{{
    {.name = "tiny", .key_bytes = 8, .value_bytes = 32},
    {.name = "small", .key_bytes = 24, .value_bytes = 128},
    {.name = "medium", .key_bytes = 64, .value_bytes = 512},
    {.name = "large", .key_bytes = 128, .value_bytes = 2048},
}};

[[nodiscard]] auto kv_profile(int idx) noexcept -> const KVProfile& {
    return kKVProfiles.at(static_cast<std::size_t>(idx));
}

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

// Cache-line isolated per-thread statistics.
struct alignas(utils::CACHE_LINE_SIZE) WorkerStats {
    std::size_t puts{0};
    std::size_t gets{0};
    std::size_t ooms{0};
    std::uint8_t failed{0};
    std::vector<double> latencies;

    WorkerStats() {
        latencies.reserve(128);
    }
};
static_assert(sizeof(WorkerStats) % utils::CACHE_LINE_SIZE == 0, "WorkerStats risks false sharing.");

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] auto pregenerate_keys(std::size_t count, std::size_t key_bytes, KeyDist dist, std::uint64_t thread_seed)
    -> std::vector<std::string> {
    std::mt19937_64 rng(thread_seed ^ 0xc0ffee'dead'beefULL);
    const std::size_t hot_boundary = std::max<std::size_t>(1, count / 5);

    std::vector<std::string> keys;
    keys.reserve(count);

    for (std::size_t i = 0; i < count; ++i) {
        std::size_t idx = i;
        switch (dist) {
            case KeyDist::Sequential:
                break;
            case KeyDist::Uniform:
                idx = rng() % count;
                break;
            case KeyDist::Hotspot: {
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

struct Fixture {
    std::unique_ptr<memory::Arena> arena;
    std::unique_ptr<memtable::SkipListMemTable> memtable;
};

[[nodiscard]] auto make_fixture(std::size_t arena_bytes) -> Fixture {
    auto exp = memory::Arena::create(make_memory_cfg(arena_bytes));
    if (!exp.has_value())
        std::terminate();
    Fixture f;
    f.arena = std::make_unique<memory::Arena>(std::move(*exp));
    f.memtable = std::make_unique<memtable::SkipListMemTable>(*f.arena, make_memtable_cfg(arena_bytes));
    return f;
}

[[nodiscard]] auto node_budget(const KVProfile& p, std::size_t count) noexcept -> std::size_t {
    const std::size_t per_node = memtable::SkipListNode::allocation_size(6, p.key_bytes, p.value_bytes);
    return (per_node * count * 5) / 4;
}

// Benchmark: Put
void run_put(benchmark::State& state) {
    const int nthreads = static_cast<int>(state.range(0));
    const auto& profile = kv_profile(static_cast<int>(state.range(1)));
    const auto dist = static_cast<KeyDist>(static_cast<int>(state.range(2)));
    const auto tc = static_cast<std::size_t>(nthreads);

    const std::size_t kOpsPerThread = effective_ops(kDefaultOpsPerThread);
    const std::size_t kLatMask = kOpsPerThread - 1;

    state.SetLabel(std::string("put/") + profile.name + "/" + dist_name(dist));

    std::vector<std::vector<std::string>> t_keys(tc);
    std::vector<std::string> t_vals(tc);
    for (std::size_t t = 0; t < tc; ++t) {
        t_keys[t] = pregenerate_keys(kOpsPerThread, profile.key_bytes, dist, t * 0x1234'5678'abcd'ef01ULL);
        t_vals[t] = std::string(profile.value_bytes, static_cast<char>('A' + t % 26));
    }

    memory::Arena* shared_arena = nullptr;
    memtable::SkipListMemTable* shared_mt = nullptr;

    std::vector<WorkerStats> w_stats(tc);
    bool stop_flag = false;

    std::barrier<> setup_barrier(nthreads + 1);
    std::barrier<> go_barrier(nthreads + 1);
    std::barrier<> done_barrier(nthreads + 1);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int ti = 0; ti < nthreads; ++ti) {
        workers.emplace_back([&, kOpsPerThread, kLatMask, t = static_cast<std::size_t>(ti)] {
            const std::vector<std::string>& keys = t_keys[t];
            const std::string& val = t_vals[t];

            while (true) {
                setup_barrier.arrive_and_wait();
                if (stop_flag)
                    break;

                if (!shared_arena || !shared_mt) {
                    w_stats[t].failed = 1;
                    go_barrier.arrive_and_wait();
                    done_barrier.arrive_and_wait();
                    continue;
                }

                memory::TLAB tlab(*shared_arena);
                w_stats[t].latencies.clear();
                w_stats[t].failed = 0;

                go_barrier.arrive_and_wait();

                for (std::size_t op = 0; op < kOpsPerThread; ++op) {
                    const bool sample = (op & kLatMask) == 0;
                    const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                    auto r = shared_mt->put(keys[op], val, tlab);

                    if (sample) {
                        w_stats[t].latencies.push_back(
                            std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
                    }
                    if (r == memtable::PutResult::OutOfMemory) {
                        w_stats[t].failed = 1;
                        break;
                    }
                    benchmark::DoNotOptimize(r);
                }
                done_barrier.arrive_and_wait();
            }
        });
    }

    std::vector<double> all_lat;
    std::size_t payload_bytes = 0;

    for (auto _ : state) {
        state.PauseTiming();

        const std::size_t cap = node_budget(profile, kOpsPerThread * tc)
                                + (tc * config::MemoryConfig::DEFAULT_TLAB_SIZE)
                                + effective_headroom(kDefaultHeadroomLarge);

        auto fix = make_fixture(cap);
        shared_arena = fix.arena.get();
        shared_mt = fix.memtable.get();

        setup_barrier.arrive_and_wait();

        state.ResumeTiming();
        go_barrier.arrive_and_wait();
        done_barrier.arrive_and_wait();
        state.PauseTiming();

        bool has_failure = false;
        for (std::size_t t = 0; t < tc; ++t) {
            if (w_stats[t].failed)
                has_failure = true;
        }

        if (has_failure) {
            state.SkipWithError("Arena OOM during put benchmark — raise STRATADB_BENCH_ARENA_HEADROOM_MIB");
            break; // RAII safe exit instead of goto
        }

        payload_bytes += kOpsPerThread * tc * (profile.key_bytes + profile.value_bytes);
        for (std::size_t t = 0; t < tc; ++t) {
            all_lat.insert(all_lat.end(), w_stats[t].latencies.begin(), w_stats[t].latencies.end());
        }

        state.ResumeTiming();
    }

    state.PauseTiming();
    stop_flag = true;
    setup_barrier.arrive_and_wait();

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

// Benchmark: Get
void run_get(benchmark::State& state) {
    const int nthreads = static_cast<int>(state.range(0));
    const auto& profile = kv_profile(static_cast<int>(state.range(1)));
    const int hit_pct = static_cast<int>(state.range(2));
    const auto tc = static_cast<std::size_t>(nthreads);

    const std::size_t kOpsPerThread = effective_ops(kDefaultOpsPerThread);
    const std::size_t kLatMask = kOpsPerThread - 1;

    state.SetLabel("get/" + std::string(profile.name) + "/hit" + std::to_string(hit_pct));

    static constexpr std::size_t kPopulateCount = 65'536;
    const std::size_t cap = node_budget(profile, kPopulateCount) + (64ULL << 20);
    auto fix = make_fixture(cap);

    {
        memory::TLAB seed_tlab(*fix.arena);
        const std::string seed_val(profile.value_bytes, 'X');
        for (std::size_t i = 0; i < kPopulateCount; ++i) {
            const auto key = make_ordered_key(i, profile.key_bytes);
            if (fix.memtable->put(key, seed_val, seed_tlab) != memtable::PutResult::Ok) {
                state.SkipWithError("Fixture population failed");
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

    std::vector<WorkerStats> w_stats(tc);
    bool stop_flag = false;

    std::barrier<> setup_barrier(nthreads + 1);
    std::barrier<> go_barrier(nthreads + 1);
    std::barrier<> done_barrier(nthreads + 1);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int ti = 0; ti < nthreads; ++ti) {
        workers.emplace_back([&, kOpsPerThread, kLatMask, t = static_cast<std::size_t>(ti)] {
            const std::vector<std::string>& keys = t_probe_keys[t];
            const memtable::SkipListMemTable& mt = *fix.memtable;

            while (true) {
                setup_barrier.arrive_and_wait();
                if (stop_flag)
                    break;

                w_stats[t].latencies.clear();

                go_barrier.arrive_and_wait();

                for (std::size_t op = 0; op < kOpsPerThread; ++op) {
                    const bool sample = (op & kLatMask) == 0;
                    const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                    auto result = mt.get(keys[op]);

                    if (sample)
                        w_stats[t].latencies.push_back(
                            std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
                    benchmark::DoNotOptimize(result);
                }
                done_barrier.arrive_and_wait();
            }
        });
    }

    std::vector<double> all_lat;

    for (auto _ : state) {
        state.PauseTiming();
        setup_barrier.arrive_and_wait();

        state.ResumeTiming();
        go_barrier.arrive_and_wait();
        done_barrier.arrive_and_wait();
        state.PauseTiming();

        for (std::size_t t = 0; t < tc; ++t) {
            all_lat.insert(all_lat.end(), w_stats[t].latencies.begin(), w_stats[t].latencies.end());
        }
        state.ResumeTiming();
    }

    state.PauseTiming();
    stop_flag = true;
    setup_barrier.arrive_and_wait();
    for (auto& w : workers)
        w.join();

    const auto lat = summarize_ext(std::move(all_lat));
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(nthreads)
                            * static_cast<std::int64_t>(kOpsPerThread));
    state.counters["p50_ns"] = lat.p50_ns;
    state.counters["p95_ns"] = lat.p95_ns;
    state.counters["p99_ns"] = lat.p99_ns;
    state.counters["hit_pct"] = static_cast<double>(hit_pct);
}

// Benchmark: Remove
void run_remove(benchmark::State& state) {
    const int nthreads = static_cast<int>(state.range(0));
    const auto& profile = kv_profile(static_cast<int>(state.range(1)));
    const auto tc = static_cast<std::size_t>(nthreads);

    const std::size_t kOpsPerThread = effective_ops(kDefaultOpsPerThread);
    const std::size_t kLatMask = kOpsPerThread - 1;

    state.SetLabel(std::string("remove/") + profile.name);

    std::vector<std::vector<std::string>> t_keys(tc);
    for (std::size_t t = 0; t < tc; ++t) {
        t_keys[t] = pregenerate_keys(kOpsPerThread, profile.key_bytes, KeyDist::Uniform, t * 0xdeadface'1234'5678ULL);
    }

    memory::Arena* shared_arena = nullptr;
    memtable::SkipListMemTable* shared_mt = nullptr;

    std::vector<WorkerStats> w_stats(tc);
    bool stop_flag = false;

    std::barrier<> setup_barrier(nthreads + 1);
    std::barrier<> go_barrier(nthreads + 1);
    std::barrier<> done_barrier(nthreads + 1);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int ti = 0; ti < nthreads; ++ti) {
        workers.emplace_back([&, kOpsPerThread, kLatMask, t = static_cast<std::size_t>(ti)] {
            const std::vector<std::string>& keys = t_keys[t];

            while (true) {
                setup_barrier.arrive_and_wait();
                if (stop_flag)
                    break;

                if (!shared_arena || !shared_mt) {
                    w_stats[t].failed = 1;
                    go_barrier.arrive_and_wait();
                    done_barrier.arrive_and_wait();
                    continue;
                }

                memory::TLAB tlab(*shared_arena);
                w_stats[t].latencies.clear();
                w_stats[t].failed = 0;

                go_barrier.arrive_and_wait();

                for (std::size_t op = 0; op < kOpsPerThread; ++op) {
                    const bool sample = (op & kLatMask) == 0;
                    const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                    auto r = shared_mt->remove(keys[op], tlab);

                    if (sample)
                        w_stats[t].latencies.push_back(
                            std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
                    if (r == memtable::PutResult::OutOfMemory) {
                        w_stats[t].failed = 1;
                        break;
                    }
                    benchmark::DoNotOptimize(r);
                }
                done_barrier.arrive_and_wait();
            }
        });
    }

    std::vector<double> all_lat;

    for (auto _ : state) {
        state.PauseTiming();

        const std::size_t cap = memtable::SkipListNode::allocation_size(1, profile.key_bytes, 0) * kOpsPerThread * tc
                                + (tc * config::MemoryConfig::DEFAULT_TLAB_SIZE)
                                + effective_headroom(kDefaultHeadroomSmall);

        auto fix = make_fixture(cap);
        shared_arena = fix.arena.get();
        shared_mt = fix.memtable.get();

        setup_barrier.arrive_and_wait();

        state.ResumeTiming();
        go_barrier.arrive_and_wait();
        done_barrier.arrive_and_wait();
        state.PauseTiming();

        bool has_failure = false;
        for (std::size_t t = 0; t < tc; ++t) {
            if (w_stats[t].failed)
                has_failure = true;
        }

        if (has_failure) {
            state.SkipWithError("Arena OOM during remove benchmark — raise STRATADB_BENCH_ARENA_HEADROOM_MIB");
            break;
        }

        for (std::size_t t = 0; t < tc; ++t) {
            all_lat.insert(all_lat.end(), w_stats[t].latencies.begin(), w_stats[t].latencies.end());
        }
        state.ResumeTiming();
    }

    state.PauseTiming();
    stop_flag = true;
    setup_barrier.arrive_and_wait();
    for (auto& w : workers)
        w.join();

    const auto lat = summarize_ext(std::move(all_lat));
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations()) * static_cast<std::int64_t>(nthreads)
                            * static_cast<std::int64_t>(kOpsPerThread));
    state.counters["p50_ns"] = lat.p50_ns;
    state.counters["p95_ns"] = lat.p95_ns;
    state.counters["p99_ns"] = lat.p99_ns;
}

// Benchmark: Mixed R/W
void run_mixed(benchmark::State& state) {
    const int nthreads = static_cast<int>(state.range(0));
    const int read_pct = static_cast<int>(state.range(1));
    const auto& profile = kv_profile(static_cast<int>(state.range(2)));
    const auto tc = static_cast<std::size_t>(nthreads);

    const std::size_t kOpsPerThread = effective_ops(kDefaultOpsPerThread);
    const std::size_t kLatMask = kOpsPerThread - 1;
    const int n_writers = std::max(1, (nthreads * (100 - read_pct)) / 100);

    state.SetLabel("mixed/" + std::string(profile.name) + "/R" + std::to_string(read_pct) + "W"
                   + std::to_string(100 - read_pct));

    static constexpr std::size_t kPopulateCount = 16'384;
    const std::size_t expected_total_keys = kPopulateCount + static_cast<std::size_t>(n_writers) * kOpsPerThread;

    std::mt19937_64 rng(0xbabecafe);
    std::vector<std::vector<std::string>> t_keys(tc);

    for (std::size_t t = 0; t < tc; ++t) {
        t_keys[t].reserve(kOpsPerThread);
        const bool is_writer = t < static_cast<std::size_t>(n_writers);

        for (std::size_t op = 0; op < kOpsPerThread; ++op) {
            if (is_writer) {
                const bool is_append = (rng() % 100) < 80;
                const std::size_t idx =
                    is_append ? (kPopulateCount + t * kOpsPerThread + op) : (rng() % kPopulateCount);
                t_keys[t].push_back(make_ordered_key(idx, profile.key_bytes));
            } else {
                t_keys[t].push_back(make_ordered_key(rng() % expected_total_keys, profile.key_bytes));
            }
        }
    }

    std::string writer_val(profile.value_bytes, 'W');

    memory::Arena* shared_arena = nullptr;
    memtable::SkipListMemTable* shared_mt = nullptr;

    std::vector<WorkerStats> w_stats(tc);
    bool stop_flag = false;

    std::barrier<> setup_barrier(nthreads + 1);
    std::barrier<> go_barrier(nthreads + 1);
    std::barrier<> done_barrier(nthreads + 1);

    std::vector<std::thread> workers;
    workers.reserve(tc);

    for (int ti = 0; ti < nthreads; ++ti) {
        workers.emplace_back([&, n_writers, kOpsPerThread, kLatMask, t = static_cast<std::size_t>(ti)] {
            const bool is_writer = t < static_cast<std::size_t>(n_writers);
            const std::vector<std::string>& keys = t_keys[t];

            while (true) {
                setup_barrier.arrive_and_wait();
                if (stop_flag)
                    break;

                if (!shared_arena || !shared_mt) {
                    w_stats[t].failed = 1;
                    go_barrier.arrive_and_wait();
                    done_barrier.arrive_and_wait();
                    continue;
                }

                memory::TLAB tlab(*shared_arena);
                w_stats[t].latencies.clear();
                w_stats[t].puts = w_stats[t].gets = w_stats[t].ooms = 0;
                w_stats[t].failed = 0;

                go_barrier.arrive_and_wait();

                if (is_writer) {
                    for (std::size_t op = 0; op < kOpsPerThread; ++op) {
                        const bool sample = (op & kLatMask) == 0;
                        const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                        auto r = shared_mt->put(keys[op], writer_val, tlab);

                        if (sample)
                            w_stats[t].latencies.push_back(
                                std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
                        if (r == memtable::PutResult::OutOfMemory)
                            ++w_stats[t].ooms;
                        else
                            ++w_stats[t].puts;

                        benchmark::DoNotOptimize(r);
                    }
                } else {
                    for (std::size_t op = 0; op < kOpsPerThread; ++op) {
                        const bool sample = (op & kLatMask) == 0;
                        const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                        auto result = shared_mt->get(keys[op]);

                        if (sample)
                            w_stats[t].latencies.push_back(
                                std::chrono::duration<double, std::nano>(Clock::now() - t0).count());
                        ++w_stats[t].gets;
                        benchmark::DoNotOptimize(result);
                    }
                }
                done_barrier.arrive_and_wait();
            }
        });
    }

    std::vector<double> all_lat;
    std::size_t sum_puts = 0, sum_gets = 0, sum_ooms = 0;

    for (auto _ : state) {
        state.PauseTiming();

        const std::size_t cap = node_budget(profile, expected_total_keys)
                                + (tc * config::MemoryConfig::DEFAULT_TLAB_SIZE)
                                + effective_headroom(kDefaultHeadroomLarge);

        auto fix = make_fixture(cap);
        shared_arena = fix.arena.get();
        shared_mt = fix.memtable.get();

        {
            memory::TLAB seed(*fix.arena);
            const std::string sv(profile.value_bytes, 'P');
            for (std::size_t i = 0; i < kPopulateCount; ++i) {
                (void)fix.memtable->put(make_ordered_key(i, profile.key_bytes), sv, seed);
            }
        }

        setup_barrier.arrive_and_wait();

        state.ResumeTiming();
        go_barrier.arrive_and_wait();
        done_barrier.arrive_and_wait();
        state.PauseTiming();

        for (std::size_t t = 0; t < tc; ++t) {
            sum_puts += w_stats[t].puts;
            sum_gets += w_stats[t].gets;
            sum_ooms += w_stats[t].ooms;
            all_lat.insert(all_lat.end(), w_stats[t].latencies.begin(), w_stats[t].latencies.end());
        }

        state.ResumeTiming();
    }

    state.PauseTiming();
    stop_flag = true;
    setup_barrier.arrive_and_wait();
    for (auto& w : workers)
        w.join();

    const auto lat = summarize_ext(std::move(all_lat));
    state.SetItemsProcessed(static_cast<std::int64_t>(sum_puts + sum_gets));
    state.counters["put_ops"] = static_cast<double>(sum_puts);
    state.counters["get_ops"] = static_cast<double>(sum_gets);
    state.counters["oom_hits"] = static_cast<double>(sum_ooms);
    state.counters["p50_ns"] = lat.p50_ns;
    state.counters["p95_ns"] = lat.p95_ns;
    state.counters["p99_ns"] = lat.p99_ns;
}

// Benchmark registration
static void BM_MemTablePut(benchmark::State& state) {
    run_put(state);
}
static void BM_MemTableGet(benchmark::State& state) {
    run_get(state);
}
static void BM_MemTableRemove(benchmark::State& state) {
    run_remove(state);
}
static void BM_MemTableMixedRW(benchmark::State& state) {
    run_mixed(state);
}

void register_put_args(benchmark::Benchmark* b) {
    b->Args({1, 1, static_cast<int>(KeyDist::Uniform)});
    b->Args({4, 1, static_cast<int>(KeyDist::Uniform)});
}

void register_get_args(benchmark::Benchmark* b) {
    b->Args({1, 1, 100});
    b->Args({4, 1, 100});
}

void register_remove_args(benchmark::Benchmark* b) {
    b->Args({1, 1});
    b->Args({4, 1});
}

void register_mixed_args(benchmark::Benchmark* b) {
    b->Args({1, 80, 1});
    b->Args({4, 80, 1});
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_MemTablePut)->Apply(stratadb::bench::register_put_args);
BENCHMARK(stratadb::bench::BM_MemTableGet)->Apply(stratadb::bench::register_get_args);
BENCHMARK(stratadb::bench::BM_MemTableRemove)->Apply(stratadb::bench::register_remove_args);
BENCHMARK(stratadb::bench::BM_MemTableMixedRW)->Apply(stratadb::bench::register_mixed_args);