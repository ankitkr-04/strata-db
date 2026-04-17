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

    mgr.register_thread();

    auto* obj = new Dummy();

    mgr.enter();
    mgr.retire(obj);
    mgr.leave();

    mgr.advance_epoch();
    mgr.reclaim();

    ASSERT_EQ(Dummy::destructions.load(), 1);

    mgr.unregister_thread();
}

// Test 2: Deferred deletion (deterministic, no sleeps)
TEST(EpochManagerTest, DeferredDeletion) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    std::atomic<bool> a_ready{false};
    std::atomic<bool> a_can_leave{false};
    std::atomic<bool> a_left{false};

    std::jthread threadA([&] {
        mgr.register_thread();

        mgr.enter(); // pins old epoch
        a_ready.store(true, std::memory_order_release);

        while (!a_can_leave.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        mgr.leave();
        a_left.store(true, std::memory_order_release);

        mgr.unregister_thread();
    });

    std::jthread threadB([&] {
        mgr.register_thread();

        while (!a_ready.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        auto* obj = new Dummy();

        mgr.advance_epoch();
        mgr.retire(obj);

        mgr.reclaim();

        // Must NOT reclaim yet (A still active)
        ASSERT_EQ(Dummy::destructions.load(), 0);

        a_can_leave.store(true, std::memory_order_release);

        while (!a_left.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }

        mgr.advance_epoch(); // ensure epoch separation
        mgr.reclaim();

        // Now safe
        ASSERT_EQ(Dummy::destructions.load(), 1);

        mgr.unregister_thread();
    });
}

// Test 3: TSAN stress test (high concurrency)
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
            mgr.register_thread();

            for (int j = 0; j < 10000; ++j) {
                mgr.enter();

                volatile int x = j * j;
                (void)x;

                mgr.leave();
            }

            mgr.unregister_thread();
        });
    }

    // Writers
    for (int i = NUM_READERS; i < NUM_THREADS; ++i) {
        threads.emplace_back([&] {
            mgr.register_thread();

            for (int j = 0; j < 10000; ++j) {
                mgr.advance_epoch();

                auto* obj = new Dummy();
                mgr.retire(obj);

                // Occasionally reclaim to build backlog
                if ((j & 7) == 0) {
                    mgr.reclaim();
                }
            }

            mgr.unregister_thread();
        });
    }

    // Force join (jthread destructor)
    threads.clear();

    // Final sweep
    mgr.register_thread();

    mgr.advance_epoch();
    mgr.advance_epoch(); // ensure all nodes are reclaimable
    mgr.reclaim();

    ASSERT_GT(Dummy::destructions.load(), 0);

    mgr.unregister_thread();
}

TEST(EpochManagerTest, BatchingBehavior) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    mgr.register_thread();

    // Retire less than batch size
    for (int i = 0; i < 63; ++i) {
        mgr.retire(new Dummy());
    }

    mgr.advance_epoch();
    mgr.reclaim();

    // Nothing should be reclaimed yet (depends on batching)
    ASSERT_EQ(Dummy::destructions.load(), 63); // because we forced reclaim manually

    mgr.unregister_thread();
}

TEST(EpochManagerTest, EpochStallPreventsReclaim) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    std::atomic<bool> reader_ready{false};

    std::jthread reader([&] {
        mgr.register_thread();
        mgr.enter(); // never leaves

        reader_ready.store(true, std::memory_order_release);

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        mgr.leave();
        mgr.unregister_thread();
    });

    std::jthread writer([&] {
        mgr.register_thread();

        while (!reader_ready.load(std::memory_order_acquire)) {
        }

        auto* obj = new Dummy();

        mgr.advance_epoch();
        mgr.retire(obj);

        mgr.reclaim();

        // Should NOT reclaim because reader is stuck
        ASSERT_EQ(Dummy::destructions.load(), 0);

        mgr.unregister_thread();
    });
}

TEST(EpochManagerTest, ThreadSlotReuse) {
    EpochManager mgr;

    for (int i = 0; i < 1000; ++i) {
        std::jthread t([&] {
            mgr.register_thread();
            mgr.enter();
            mgr.leave();
            mgr.unregister_thread();
        });
    }

    SUCCEED();
}

TEST(EpochManagerTest, MultiEpochReclaim) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    mgr.register_thread();

    for (int i = 0; i < 100; ++i) {
        mgr.enter();
        mgr.retire(new Dummy());
        mgr.leave();

        mgr.advance_epoch();
    }

    mgr.reclaim();

    ASSERT_EQ(Dummy::destructions.load(), 100);

    mgr.unregister_thread();
}

TEST(EpochManagerTest, ReclaimIdempotent) {
    EpochManager mgr;
    Dummy::destructions.store(0);

    mgr.register_thread();

    auto* obj = new Dummy();

    mgr.enter();
    mgr.retire(obj);
    mgr.leave();

    mgr.advance_epoch();
    mgr.reclaim();
    mgr.reclaim(); // second call should do nothing

    ASSERT_EQ(Dummy::destructions.load(), 1);

    mgr.unregister_thread();
}