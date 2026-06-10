#include "../support/common.hpp"
#include "stratadb/memory/epoch_manager.hpp"

#include <atomic>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using stratadb::memory::EpochManager;
using stratadb::test::DestructionSpy;
using stratadb::test::do_not_optimize;

namespace {

using Dummy = DestructionSpy;

class EpochManagerTest : public ::testing::Test {
  protected:
    EpochManager mgr{};
    EpochManager::ThreadRegistrationGuard tg{mgr};

    void SetUp() override {
        Dummy::reset();
    }
};

} // namespace

TEST_F(EpochManagerTest, SingleThreadedReclaim) {
    auto* obj = new Dummy();

    {
        EpochManager::ReadGuard guard(mgr);
        mgr.retire(obj);
    }

    mgr.quiescent_reclaim();

    ASSERT_EQ(Dummy::count.load(), 1);
}

TEST_F(EpochManagerTest, DeferredDeletion) {
    std::atomic<bool> a_ready{false};
    std::atomic<bool> a_can_leave{false};
    std::atomic<bool> a_left{false};

    std::jthread thread_a([&] -> void {
        EpochManager::ThreadRegistrationGuard local_tg(mgr);

        {
            EpochManager::ReadGuard guard(mgr);
            a_ready.store(true, std::memory_order_release);

            while (!a_can_leave.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        }

        a_left.store(true, std::memory_order_release);
    });

    std::jthread thread_b([&] -> void {
        EpochManager::ThreadRegistrationGuard local_tg(mgr);

        while (!a_ready.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        mgr.retire(new Dummy());
        mgr.quiescent_reclaim();

        ASSERT_EQ(Dummy::count.load(), 0);

        a_can_leave.store(true, std::memory_order_release);

        while (!a_left.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        mgr.quiescent_reclaim();

        ASSERT_EQ(Dummy::count.load(), 1);
    });
}

TEST_F(EpochManagerTest, TSANStress) {
    constexpr int kNumThreads = 16;
    constexpr int kNumReaders = 8;

    std::vector<std::jthread> threads;
    threads.reserve(kNumThreads);

    for (int i = 0; i < kNumReaders; ++i) {
        threads.emplace_back([&] -> void {
            EpochManager::ThreadRegistrationGuard local_tg(mgr);

            for (int j = 0; j < 10000; ++j) {
                EpochManager::ReadGuard guard(mgr);
                auto x = j * j;
                do_not_optimize(x);
            }
        });
    }

    for (int i = kNumReaders; i < kNumThreads; ++i) {
        threads.emplace_back([&] {
            EpochManager::ThreadRegistrationGuard local_tg(mgr);

            for (int j = 0; j < 10000; ++j) {
                mgr.retire(new Dummy());

                if ((j & 7) == 0) {
                    mgr.quiescent_reclaim();
                }
            }
        });
    }

    threads.clear();

    mgr.quiescent_reclaim();
    mgr.quiescent_reclaim();

    ASSERT_GT(Dummy::count.load(), 0);
}

TEST_F(EpochManagerTest, BatchingBehavior) {
    for (int i = 0; i < 63; ++i) {
        mgr.retire(new Dummy());
    }

    mgr.quiescent_reclaim();

    ASSERT_EQ(Dummy::count.load(), 63);
}

TEST_F(EpochManagerTest, EpochStallPreventsReclaim) {
    std::atomic<bool> reader_ready{false};

    std::jthread reader([&] -> void {
        EpochManager::ThreadRegistrationGuard local_tg(mgr);

        {
            EpochManager::ReadGuard guard(mgr);
            reader_ready.store(true, std::memory_order_release);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });

    std::jthread writer([&] -> void {
        EpochManager::ThreadRegistrationGuard local_tg(mgr);

        while (!reader_ready.load(std::memory_order_acquire)) {
        }

        mgr.retire(new Dummy());
        mgr.quiescent_reclaim();

        ASSERT_EQ(Dummy::count.load(), 0);
    });
}

TEST_F(EpochManagerTest, ThreadSlotReuse) {
    for (int i = 0; i < 1000; ++i) {
        std::jthread t([&] {
            EpochManager::ThreadRegistrationGuard local_tg(mgr);
            EpochManager::ReadGuard guard(mgr);
        });
    }

    SUCCEED();
}

TEST_F(EpochManagerTest, MultiEpochReclaim) {
    for (int i = 0; i < 100; ++i) {
        {
            EpochManager::ReadGuard guard(mgr);
            mgr.retire(new Dummy());
        }
        mgr.quiescent_reclaim();
    }

    ASSERT_EQ(Dummy::count.load(), 100);
}

TEST_F(EpochManagerTest, ReclaimIdempotent) {
    auto* obj = new Dummy();

    {
        EpochManager::ReadGuard guard(mgr);
        mgr.retire(obj);
    }

    mgr.quiescent_reclaim();
    mgr.quiescent_reclaim();

    ASSERT_EQ(Dummy::count.load(), 1);
}
