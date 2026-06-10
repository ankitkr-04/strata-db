#include "../support/common.hpp"
#include "stratadb/config/config_manager.hpp"
#include "stratadb/memory/epoch_manager.hpp"

#include <atomic>
#include <cstdint>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using stratadb::config::ConfigManager;
using stratadb::config::ImmutableConfig;
using stratadb::config::MutableConfig;
using stratadb::memory::EpochManager;
using stratadb::test::DestructionSpy;
using stratadb::test::do_not_optimize;

namespace {

struct TrackableConfig : public MutableConfig, public DestructionSpy {};

} // namespace

TEST(ConfigManagerTest, BasicRead) {
    EpochManager epoch;
    EpochManager::ThreadRegistrationGuard tg(epoch);

    MutableConfig mut{};
    ImmutableConfig imm{};

    ConfigManager mgr(imm, mut, epoch);

    {
        auto guard = mgr.get_mutable();
        auto& cfg = guard.get();

        ASSERT_EQ(cfg.background_compaction_threads, MutableConfig::default_compaction_threads());
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

    ASSERT_TRUE(mgr.update_mutable(new_cfg).has_value());

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

    constexpr int kNumThreads = 8;
    constexpr int kIters = 10000;

    std::vector<std::jthread> threads;
    threads.reserve(kNumThreads);

    for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([&] {
            EpochManager::ThreadRegistrationGuard local_tg(epoch);

            for (int j = 0; j < kIters; ++j) {
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

    constexpr int kNumReaders = 8;
    constexpr int kNumWrites = 5000;

    std::atomic<bool> start{false};

    std::vector<std::jthread> threads;
    threads.reserve(kNumReaders + 1);

    for (int i = 0; i < kNumReaders; ++i) {
        threads.emplace_back([&] {
            EpochManager::ThreadRegistrationGuard local_tg(epoch);

            while (!start.load(std::memory_order_acquire)) {
            }

            for (int j = 0; j < kNumWrites; ++j) {
                auto guard = mgr.get_mutable();
                auto x = guard->background_compaction_threads;
                do_not_optimize(x);
            }
        });
    }

    threads.emplace_back([&] {
        EpochManager::ThreadRegistrationGuard local_tg(epoch);

        start.store(true, std::memory_order_release);

        for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(kNumWrites); ++i) {
            MutableConfig cfg{};
            cfg.background_compaction_threads = i;

            ASSERT_TRUE(mgr.update_mutable(cfg).has_value());

            if ((i & 7) == 0) {
                epoch.quiescent_reclaim();
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

    constexpr int kNumThreads = 12;
    constexpr int kIters = 20000;

    std::vector<std::jthread> threads;
    threads.reserve(kNumThreads + 1);

    for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([&] {
            EpochManager::ThreadRegistrationGuard local_tg(epoch);

            for (int j = 0; j < kIters; ++j) {
                auto guard = mgr.get_mutable();
                auto x = guard->background_compaction_threads;
                do_not_optimize(x);

                if ((j & 15) == 0) {
                    epoch.quiescent_reclaim();
                }
            }
        });
    }

    threads.emplace_back([&] {
        EpochManager::ThreadRegistrationGuard local_tg(epoch);

        for (std::uint32_t i = 0; i < static_cast<std::uint32_t>(kIters); ++i) {
            MutableConfig cfg{};
            cfg.background_compaction_threads = i;
            ASSERT_TRUE(mgr.update_mutable(cfg).has_value());
        }
    });
}

TEST(ConfigManagerTest, ReclamationOccurs) {
    EpochManager epoch;
    EpochManager::ThreadRegistrationGuard tg(epoch);

    TrackableConfig::reset();

    ImmutableConfig imm{};
    TrackableConfig mut{};

    ConfigManager mgr(imm, mut, epoch);

    for (int i = 0; i < 100; ++i) {
        TrackableConfig cfg{};
        ASSERT_TRUE(mgr.update_mutable(cfg).has_value());
        epoch.quiescent_reclaim();
    }

    ASSERT_GT(TrackableConfig::count.load(), 0);
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
    ASSERT_TRUE(mgr.update_mutable(new_cfg).has_value());

    auto* ptr2 = &guard.get();

    ASSERT_EQ(ptr1, ptr2);
}
