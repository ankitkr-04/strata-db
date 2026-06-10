#include "../support/test_config.hpp"
#include "stratadb/memory/block_pool.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <gtest/gtest.h>
#include <mutex>
#include <set>
#include <span>
#include <thread>
#include <vector>

namespace stratadb::memory::test {

struct BlockPoolTestPeer {
    static auto tail(BlockPool& pool) -> auto& {
        return pool.tail_;
    }
    static auto head(BlockPool& pool) -> auto& {
        return pool.head_;
    }
};

class BlockPoolTest : public ::testing::Test {
  protected:
    BlockPoolTest()
        : pool_(stratadb::test::test_block_pool_config(32768, 4096)) {}

    BlockPool pool_;
};

TEST_F(BlockPoolTest, MaxCapacityExhaustionAndThreadStaging) {
    std::vector<std::span<std::byte>> acquired_blocks;

    acquired_blocks.reserve(pool_.capacity());

    for (size_t i = 0; i < pool_.capacity(); ++i) {
        acquired_blocks.push_back(pool_.acquire_block());
    }

    std::atomic<bool> thread_blocked{false};
    std::atomic<bool> thread_exited{false};
    std::span<std::byte> delayed_block;

    std::jthread worker([this, &thread_blocked, &thread_exited, &delayed_block]() {
        thread_blocked.store(true, std::memory_order_release);
        delayed_block = pool_.acquire_block();
        thread_exited.store(true, std::memory_order_release);
    });

    while (!thread_blocked.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(thread_exited.load(std::memory_order_acquire));

    auto recycled_span = acquired_blocks.back();
    acquired_blocks.pop_back();
    pool_.release_block(recycled_span);

    worker.join();
    EXPECT_TRUE(thread_exited.load(std::memory_order_acquire));
    EXPECT_EQ(delayed_block.data(), recycled_span.data());
    EXPECT_EQ(delayed_block.size(), recycled_span.size());
}

TEST_F(BlockPoolTest, RingBufferIndexMaskingWrapAround) {
    constexpr size_t ITERATIONS = 1'000'000;

    for (size_t i = 0; i < ITERATIONS; ++i) {
        auto block = pool_.acquire_block();
        ASSERT_NE(block.data(), nullptr);
        pool_.release_block(block);
    }
    SUCCEED();
}

TEST_F(BlockPoolTest, FutexSpuriousWakeupResiliency) {
    std::vector<std::span<std::byte>> acquired_blocks;
    acquired_blocks.reserve(pool_.capacity());

    for (size_t i = 0; i < pool_.capacity(); ++i) {
        acquired_blocks.push_back(pool_.acquire_block());
    }

    std::atomic<bool> consumer_waiting{false};
    std::atomic<bool> custom_wakeup_passed{false};

    std::jthread consumer([this, &consumer_waiting, &custom_wakeup_passed]() {
        consumer_waiting.store(true, std::memory_order_release);
        auto block = pool_.acquire_block();
        EXPECT_NE(block.data(), nullptr);
        custom_wakeup_passed.store(true, std::memory_order_release);
    });

    while (!consumer_waiting.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    BlockPoolTestPeer::tail(pool_).notify_one();

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(custom_wakeup_passed.load(std::memory_order_acquire));

    pool_.release_block(acquired_blocks.back());
    consumer.join();
    EXPECT_TRUE(custom_wakeup_passed.load(std::memory_order_acquire));
}

TEST_F(BlockPoolTest, PointerToIDMappingBoundaryVerification) {
    std::vector<std::span<std::byte>> blocks;
    blocks.reserve(pool_.capacity());
    for (size_t i = 0; i < pool_.capacity(); ++i) {
        blocks.push_back(pool_.acquire_block());
    }

    std::ranges::sort(blocks, [](const auto& a, const auto& b) -> auto { return a.data() < b.data(); });

    auto boundary_a = blocks.front();
    auto boundary_b = blocks.back();

    pool_.release_block(boundary_a);
    pool_.release_block(boundary_b);

    auto re_a = pool_.acquire_block();
    auto re_b = pool_.acquire_block();

    EXPECT_TRUE((re_a.data() == boundary_a.data() && re_b.data() == boundary_b.data())
                || (re_a.data() == boundary_b.data() && re_b.data() == boundary_a.data()));
}

TEST_F(BlockPoolTest, HighContentionSymmetricMultiConsumerStress) {
    constexpr int CONSUMER_THREADS = 16;
    constexpr int BLOCKS_PER_THREAD = 1000;

    std::atomic<bool> start_signal{false};
    std::vector<std::jthread> consumers;

    std::mutex tracking_mutex;
    std::set<void*> active_pointers;
    std::atomic<size_t> total_acquired{0};

    std::atomic<bool> stop_flusher{false};
    std::vector<std::span<std::byte>> transfer_queue;
    std::mutex queue_mutex;

    std::jthread flusher([this, &stop_flusher, &transfer_queue, &queue_mutex]() {
        while (!stop_flusher.load(std::memory_order_acquire) || !transfer_queue.empty()) {
            std::span<std::byte> block_to_release;
            {
                std::lock_guard<std::mutex> lock(queue_mutex);
                if (!transfer_queue.empty()) {
                    block_to_release = transfer_queue.back();
                    transfer_queue.pop_back();
                }
            }
            if (block_to_release.data() != nullptr) {
                pool_.release_block(block_to_release);
            } else {
                std::this_thread::yield();
            }
        }
    });

    for (int i = 0; i < CONSUMER_THREADS; ++i) {
        consumers.emplace_back(
            [this, &start_signal, &tracking_mutex, &active_pointers, &total_acquired, &queue_mutex, &transfer_queue]()
                -> void {
                while (!start_signal.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }

                for (int j = 0; j < BLOCKS_PER_THREAD; ++j) {
                    auto block = pool_.acquire_block();

                    {
                        std::lock_guard<std::mutex> lock(tracking_mutex);
                        auto [iter, inserted] = active_pointers.insert(block.data());
                        EXPECT_TRUE(inserted);
                    }

                    total_acquired.fetch_add(1, std::memory_order_relaxed);

                    {
                        std::lock_guard<std::mutex> lock(tracking_mutex);
                        active_pointers.erase(block.data());
                    }

                    {
                        std::lock_guard<std::mutex> lock(queue_mutex);
                        transfer_queue.push_back(block);
                    }
                }
            });
    }

    start_signal.store(true, std::memory_order_release);
    consumers.clear(); // Joins all consumers

    stop_flusher.store(true, std::memory_order_release);
    flusher.join();

    EXPECT_EQ(total_acquired.load(), CONSUMER_THREADS * BLOCKS_PER_THREAD);
}

} // namespace stratadb::memory::test
