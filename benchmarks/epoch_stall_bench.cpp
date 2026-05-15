// benchmarks/epoch_stall_bench.cpp
//
// Epoch manager stalled-reader stress test:
//   - one reader enters an epoch and parks
//   - four writers aggressively retire nodes while reclamation is blocked
//   - writer latency is reported for the first and last 10% of retire operations

#include "benchmark_common.hpp"
#include "stratadb/memory/epoch_manager.hpp"

#include <array>
#include <atomic>
#include <benchmark/benchmark.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

namespace stratadb::bench {
namespace {

using Clock = std::chrono::steady_clock;

constexpr int kWriterThreads = 4;
constexpr std::size_t kDefaultWriterOps = 131'072;
constexpr std::size_t kSampleStride = 64;

struct RetiredPayload {
    std::array<std::byte, 64> bytes{};
};

struct WriterSamples {
    std::vector<double> first;
    std::vector<double> last;

    WriterSamples() {
        first.reserve(256);
        last.reserve(256);
    }
};

[[nodiscard]] auto summarize_p99(const std::vector<WriterSamples>& samples, bool last) -> double {
    std::vector<double> merged;
    for (const auto& sample : samples) {
        const auto& src = last ? sample.last : sample.first;
        merged.insert(merged.end(), src.begin(), src.end());
    }
    return summarize_ext(std::move(merged)).p99_ns;
}

static void BM_EpochStalledReader(benchmark::State& state) {
    const std::size_t ops_per_writer = effective_ops(kDefaultWriterOps);
    const std::size_t first_cutoff = std::max<std::size_t>(1, ops_per_writer / 10);
    const std::size_t last_start = ops_per_writer - first_cutoff;

    std::size_t total_retired = 0;
    double first_p99_total = 0.0;
    double last_p99_total = 0.0;

    for (auto _ : state) {
        state.PauseTiming();

        memory::EpochManager manager;
        std::atomic<bool> reader_ready{false};
        std::atomic<bool> release_reader{false};
        std::atomic<bool> go{false};
        std::atomic<int> writers_ready{0};
        std::atomic<int> writers_done{0};
        std::atomic<bool> registration_failed{false};

        std::thread reader([&] {
            memory::EpochManager::ThreadRegistrationGuard registration(manager);
            if (!registration.is_registered()) {
                registration_failed.store(true, std::memory_order_release);
                return;
            }

            memory::EpochManager::ReadGuard guard(manager);
            reader_ready.store(true, std::memory_order_release);
            while (!release_reader.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        });

        while (!reader_ready.load(std::memory_order_acquire) && !registration_failed.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        std::vector<WriterSamples> samples(static_cast<std::size_t>(kWriterThreads));
        std::vector<std::thread> writers;
        writers.reserve(static_cast<std::size_t>(kWriterThreads));

        for (int writer_index = 0; writer_index < kWriterThreads; ++writer_index) {
            writers.emplace_back([&, slot = static_cast<std::size_t>(writer_index)] {
                memory::EpochManager::ThreadRegistrationGuard registration(manager);
                if (!registration.is_registered()) {
                    registration_failed.store(true, std::memory_order_release);
                    return;
                }

                writers_ready.fetch_add(1, std::memory_order_acq_rel);
                while (!go.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                for (std::size_t op = 0; op < ops_per_writer; ++op) {
                    auto* payload = new (std::nothrow) RetiredPayload();
                    if (!payload) {
                        registration_failed.store(true, std::memory_order_release);
                        break;
                    }

                    const bool in_first_decile = op < first_cutoff;
                    const bool in_last_decile = op >= last_start;
                    const bool sample = (in_first_decile || in_last_decile)
                                        && ((((op + 1) & (kSampleStride - 1)) == 0) || op == last_start);
                    const Clock::time_point t0 = sample ? Clock::now() : Clock::time_point{};

                    manager.retire(payload);

                    if (sample) {
                        const double ns = std::chrono::duration<double, std::nano>(Clock::now() - t0).count();
                        if (in_first_decile) {
                            samples[slot].first.push_back(ns);
                        } else {
                            samples[slot].last.push_back(ns);
                        }
                    }
                }

                writers_done.fetch_add(1, std::memory_order_acq_rel);
            });
        }

        while (writers_ready.load(std::memory_order_acquire) != kWriterThreads
               && !registration_failed.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        if (registration_failed.load(std::memory_order_acquire)) {
            release_reader.store(true, std::memory_order_release);
            go.store(true, std::memory_order_release);
            for (auto& writer : writers) {
                if (writer.joinable()) {
                    writer.join();
                }
            }
            if (reader.joinable()) {
                reader.join();
            }
            state.SkipWithError("EpochManager registration or payload allocation failed");
            state.ResumeTiming();
            break;
        }

        state.ResumeTiming();
        go.store(true, std::memory_order_release);
        while (writers_done.load(std::memory_order_acquire) != kWriterThreads) {
            std::this_thread::yield();
        }
        state.PauseTiming();

        release_reader.store(true, std::memory_order_release);
        for (auto& writer : writers) {
            writer.join();
        }
        reader.join();

        total_retired += ops_per_writer * static_cast<std::size_t>(kWriterThreads);
        first_p99_total += summarize_p99(samples, false);
        last_p99_total += summarize_p99(samples, true);

        state.ResumeTiming();
    }

    const double denom = static_cast<double>(std::max<std::int64_t>(1, state.iterations()));
    const double first_p99 = first_p99_total / denom;
    const double last_p99 = last_p99_total / denom;

    state.SetItemsProcessed(static_cast<std::int64_t>(total_retired));
    state.counters["writer_threads"] = static_cast<double>(kWriterThreads);
    state.counters["ops_per_writer"] = static_cast<double>(ops_per_writer);
    state.counters["first_10pct_retire_p99_ns"] = first_p99;
    state.counters["last_10pct_retire_p99_ns"] = last_p99;
    state.counters["stalled_reader_p99_ratio"] = first_p99 == 0.0 ? 0.0 : last_p99 / first_p99;
}

} // namespace
} // namespace stratadb::bench

BENCHMARK(stratadb::bench::BM_EpochStalledReader)->UseRealTime();
