#include "stratadb/memory/epoch_manager.hpp"

#include <atomic>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using stratadb::memory::EpochManager;

// Test helper
struct Dummy {
    static std::atomic<int> destructions;

    ~Dummy() {
        destructions.fetch_add(1, std::memory_order_relaxed);
    }
};

std::atomic<int> Dummy::destructions{0};

// Test 1: Single-threaded correctness
TEST(EpochManagerTest, SingleThreadedReclaim) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    EpochManager::ThreadGuard tg(mgr);

    auto* obj = new Dummy();

    {
        EpochManager::ReadGuard guard(mgr);
        mgr.retire(obj);
    }

    mgr.advance_epoch();
    mgr.reclaim();

    ASSERT_EQ(Dummy::destructions.load(), 1);
}

// Test 2: Deferred deletion
TEST(EpochManagerTest, DeferredDeletion) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    std::atomic<bool> a_ready{false};
    std::atomic<bool> a_can_leave{false};
    std::atomic<bool> a_left{false};

    std::jthread threadA([&] {
        EpochManager::ThreadGuard tg(mgr);

        {
            EpochManager::ReadGuard guard(mgr);
            a_ready.store(true, std::memory_order_release);

            while (!a_can_leave.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        }

        a_left.store(true, std::memory_order_release);
    });

    std::jthread threadB([&] {
        EpochManager::ThreadGuard tg(mgr);

        while (!a_ready.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        auto* obj = new Dummy();

        mgr.advance_epoch();
        mgr.retire(obj);
        mgr.reclaim();

        ASSERT_EQ(Dummy::destructions.load(), 0);

        a_can_leave.store(true, std::memory_order_release);

        while (!a_left.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        mgr.advance_epoch();
        mgr.reclaim();

        ASSERT_EQ(Dummy::destructions.load(), 1);
    });
}

// Test 3: TSAN stress (REAL contention)
TEST(EpochManagerTest, TSANStress) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    constexpr int NUM_THREADS = 16;
    constexpr int NUM_READERS = 8;

    std::vector<std::jthread> threads;
    threads.reserve(NUM_THREADS);

    // Readers
    for (int i = 0; i < NUM_READERS; ++i) {
        threads.emplace_back([&] {
            EpochManager::ThreadGuard tg(mgr);

            for (int j = 0; j < 10000; ++j) {
                EpochManager::ReadGuard guard(mgr);
                volatile int x = j * j;
                (void)x;
            }
        });
    }

    // Writers (FIXED)
    for (int i = NUM_READERS; i < NUM_THREADS; ++i) {
        threads.emplace_back([&] {
            EpochManager::ThreadGuard tg(mgr);

            for (int j = 0; j < 10000; ++j) {
                mgr.advance_epoch();

                auto* obj = new Dummy();
                mgr.retire(obj);

                if ((j & 7) == 0) {
                    mgr.reclaim();
                }
            }
        });
    }

    threads.clear();

    {
        EpochManager::ThreadGuard tg(mgr);

        mgr.advance_epoch();
        mgr.advance_epoch();
        mgr.reclaim();

        ASSERT_GT(Dummy::destructions.load(), 0);
    }
}

// Test 4: Batching
TEST(EpochManagerTest, BatchingBehavior) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    EpochManager::ThreadGuard tg(mgr);

    for (int i = 0; i < 63; ++i) {
        mgr.retire(new Dummy());
    }

    mgr.advance_epoch();
    mgr.reclaim();

    ASSERT_EQ(Dummy::destructions.load(), 63);
}

// Test 5: Epoch stall
TEST(EpochManagerTest, EpochStallPreventsReclaim) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    std::atomic<bool> reader_ready{false};

    std::jthread reader([&] {
        EpochManager::ThreadGuard tg(mgr);

        {
            EpochManager::ReadGuard guard(mgr);
            reader_ready.store(true, std::memory_order_release);
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });

    std::jthread writer([&] {
        EpochManager::ThreadGuard tg(mgr);

        while (!reader_ready.load(std::memory_order_acquire)) {
        }

        auto* obj = new Dummy();

        mgr.advance_epoch();
        mgr.retire(obj);
        mgr.reclaim();

        ASSERT_EQ(Dummy::destructions.load(), 0);
    });
}

// Test 6: Thread slot reuse
TEST(EpochManagerTest, ThreadSlotReuse) {
    EpochManager mgr;

    for (int i = 0; i < 1000; ++i) {
        std::jthread t([&] {
            EpochManager::ThreadGuard tg(mgr);
            EpochManager::ReadGuard guard(mgr);
        });
    }

    SUCCEED();
}

// Test 7: Multi-epoch reclaim
TEST(EpochManagerTest, MultiEpochReclaim) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    EpochManager::ThreadGuard tg(mgr);

    for (int i = 0; i < 100; ++i) {
        {
            EpochManager::ReadGuard guard(mgr);
            mgr.retire(new Dummy());
        }
        mgr.advance_epoch();
    }

    mgr.reclaim();

    ASSERT_EQ(Dummy::destructions.load(), 100);
}

// Test 8: Idempotent reclaim
TEST(EpochManagerTest, ReclaimIdempotent) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    EpochManager::ThreadGuard tg(mgr);

    auto* obj = new Dummy();

    {
        EpochManager::ReadGuard guard(mgr);
        mgr.retire(obj);
    }

    mgr.advance_epoch();
    mgr.reclaim();
    mgr.reclaim();

    ASSERT_EQ(Dummy::destructions.load(), 1);
}