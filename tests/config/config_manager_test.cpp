#include "stratadb/config/config_manager.hpp"
#include "stratadb/memory/epoch_manager.hpp"

#include <atomic>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using stratadb::config::ConfigManager;
using stratadb::config::ImmutableConfig;
using stratadb::config::MutableConfig;
using stratadb::memory::EpochManager;

namespace {

template <typename T>
void do_not_optimize(const T& value) noexcept {
#if defined(__GNUC__) || defined(__clang__)
    __asm__ __volatile__("" : : "g"(value) : "memory");
#else
    (void)value;
    std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
}

} // namespace

struct TrackableConfig : public MutableConfig {
    static std::atomic<int> destructions;

    ~TrackableConfig() {
        destructions.fetch_add(1, std::memory_order_relaxed);
    }
};

std::atomic<int> TrackableConfig::destructions{0};

TEST(ConfigManagerTest, BasicRead) {
    EpochManager epoch;
    EpochManager::ThreadRegistrationGuard tg(epoch);

    MutableConfig mut{};
    ImmutableConfig imm{};

    ConfigManager mgr(imm, mut, epoch);

    {
        auto guard = mgr.get_mutable();
        auto& cfg = guard.get();

        ASSERT_EQ(cfg.background_compaction_threads, 2);
    }
}

TEST(ConfigManagerTest, UpdateVisibility) {
    EpochManager epoch;
    EpochManager::ThreadRegistrationGuard tg(epoch);

    MutableConfig mut{};
    ImmutableConfig imm{};

    ConfigManager mgr(imm, mut, epoch);

    MutableConfig new_cfg{};
    new_cfg.background_compaction_threads = 8;

    mgr.update_mutable(new_cfg);

    {
        auto guard = mgr.get_mutable();
        ASSERT_EQ(guard->background_compaction_threads, 8);
    }
}

TEST(ConfigManagerTest, ConcurrentReaders) {
    EpochManager epoch;
    EpochManager::ThreadRegistrationGuard tg(epoch);

    MutableConfig mut{};
    ImmutableConfig imm{};
    ConfigManager mgr(imm, mut, epoch);

    constexpr int NUM_THREADS = 8;
    constexpr int ITERS = 10000;

    std::vector<std::jthread> threads;

    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&] {
            EpochManager::ThreadRegistrationGuard local_tg(epoch);

            for (int j = 0; j < ITERS; ++j) {
                auto guard = mgr.get_mutable();
                auto x = guard->background_compaction_threads;
                do_not_optimize(x);
            }
        });
    }
}

TEST(ConfigManagerTest, ConcurrentReadWrite) {
    EpochManager epoch;
    EpochManager::ThreadRegistrationGuard tg(epoch);

    MutableConfig mut{};
    ImmutableConfig imm{};
    ConfigManager mgr(imm, mut, epoch);

    constexpr int NUM_READERS = 8;
    constexpr int NUM_WRITES = 5000;

    std::atomic<bool> start{false};

    std::vector<std::jthread> threads;

    for (int i = 0; i < NUM_READERS; ++i) {
        threads.emplace_back([&] {
            EpochManager::ThreadRegistrationGuard local_tg(epoch);

            while (!start.load(std::memory_order_acquire)) {
            }

            for (int j = 0; j < NUM_WRITES; ++j) {
                auto guard = mgr.get_mutable();
                auto x = guard->background_compaction_threads;
                do_not_optimize(x);
            }
        });
    }

    threads.emplace_back([&] {
        EpochManager::ThreadRegistrationGuard local_tg(epoch);

        start.store(true, std::memory_order_release);

        for (int i = 0; i < NUM_WRITES; ++i) {
            MutableConfig cfg{};
            cfg.background_compaction_threads = i;

            mgr.update_mutable(cfg);

            if ((i & 7) == 0) {
                epoch.advance_epoch();
                epoch.reclaim();
            }
        }
    });
}

TEST(ConfigManagerTest, NoUseAfterFreeStress) {
    EpochManager epoch;
    EpochManager::ThreadRegistrationGuard tg(epoch);

    MutableConfig mut{};
    ImmutableConfig imm{};
    ConfigManager mgr(imm, mut, epoch);

    constexpr int NUM_THREADS = 12;
    constexpr int ITERS = 20000;

    std::vector<std::jthread> threads;

    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&] {
            EpochManager::ThreadRegistrationGuard local_tg(epoch);

            for (int j = 0; j < ITERS; ++j) {
                auto guard = mgr.get_mutable();

                auto x = guard->background_compaction_threads;
                do_not_optimize(x);

                if ((j & 15) == 0) {
                    epoch.advance_epoch();
                    epoch.reclaim();
                }
            }
        });
    }

    threads.emplace_back([&] {
        EpochManager::ThreadRegistrationGuard local_tg(epoch);

        for (int i = 0; i < ITERS; ++i) {
            MutableConfig cfg{};
            cfg.background_compaction_threads = i;
            mgr.update_mutable(cfg);
        }
    });
}

TEST(ConfigManagerTest, ReclamationOccurs) {
    EpochManager epoch;
    EpochManager::ThreadRegistrationGuard tg(epoch);

    TrackableConfig::destructions.store(0);

    ImmutableConfig imm{};
    TrackableConfig mut{};

    ConfigManager mgr(imm, mut, epoch);

    for (int i = 0; i < 100; ++i) {
        TrackableConfig cfg{};
        mgr.update_mutable(cfg);
        epoch.advance_epoch();
    }

    epoch.reclaim();

    ASSERT_GT(TrackableConfig::destructions.load(), 0);
}

TEST(ConfigManagerTest, PointerStability) {
    EpochManager epoch;
    EpochManager::ThreadRegistrationGuard tg(epoch);

    MutableConfig mut{};
    ImmutableConfig imm{};
    ConfigManager mgr(imm, mut, epoch);

    auto guard = mgr.get_mutable();
    auto* ptr1 = &guard.get();

    MutableConfig new_cfg{};
    mgr.update_mutable(new_cfg);

    auto* ptr2 = &guard.get();

    ASSERT_EQ(ptr1, ptr2);
}
