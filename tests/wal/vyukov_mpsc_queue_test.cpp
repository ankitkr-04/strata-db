
#include "stratadb/wal/queue/vyukov_mpsc_queue.hpp"
#include "stratadb/wal/types.hpp"

#include <atomic>
#include <gtest/gtest.h>
#include <set>
#include <thread>
#include <vector>

using namespace stratadb::wal;

// Helpers
namespace {

// Allocate a node whose pool_managed flag is false so the test owns lifetime.
MpscNode* make_node() {
    auto* n = new MpscNode{};
    n->pool_managed = false;
    return n;
}

// Pop all available items from the queue into `out`.
// Returns when pop() yields an empty result.
void drain(VyukovMpscQueue& q, std::vector<MpscNode*>& out) {
    while (true) {
        auto [payload, free_node] = q.pop();
        if (!payload)
            break;
        out.push_back(payload);
        // free_node is the old dummy head — delete it to avoid leaks.
        if (free_node && free_node != payload)
            delete free_node;
    }
}

} // namespace

// Tests

TEST(VyukovMpscQueue, EmptyQueueReturnsNull) {
    VyukovMpscQueue q;
    auto [payload, free_node] = q.pop();
    EXPECT_EQ(payload, nullptr);
    EXPECT_EQ(free_node, nullptr);
}

TEST(VyukovMpscQueue, SingleThreadFIFO) {
    VyukovMpscQueue q;

    constexpr int N = 100;
    std::vector<MpscNode*> nodes;
    nodes.reserve(N);
    for (int i = 0; i < N; ++i)
        nodes.push_back(make_node());

    for (auto* n : nodes)
        q.push(n);

    std::vector<MpscNode*> received;
    drain(q, received);

    ASSERT_EQ(static_cast<int>(received.size()), N);

    // FIFO: the order in which payloads arrive must match the push order.
    for (std::size_t i = 0; i < N; ++i) {
        EXPECT_EQ(received[i], nodes[i]) << "Order violation at index " << i;
    }

    for (auto* n : nodes)
        delete n;
}

TEST(VyukovMpscQueue, MultiProducerStress) {
    VyukovMpscQueue q;

    constexpr int kProducers = 16;
    constexpr int kPerProducer = 10'000;
    constexpr int kTotal = kProducers * kPerProducer;

    // Pre-allocate all nodes so we can track them by address.
    std::vector<MpscNode*> all_nodes;
    all_nodes.reserve(kTotal);
    for (int i = 0; i < kTotal; ++i)
        all_nodes.push_back(make_node());

    std::atomic<bool> start{false};

    // Consumer thread.
    std::atomic<int> consumed{0};
    std::set<MpscNode*> seen;
    std::atomic<bool> duplicate{false};
    std::atomic<bool> stop_consumer{false};

    std::thread consumer([&] {
        while (!stop_consumer.load(std::memory_order_acquire) || consumed.load(std::memory_order_relaxed) < kTotal) {
            auto [payload, free_node] = q.pop();
            if (!payload) {
                std::this_thread::yield();
                continue;
            }
            if (!seen.insert(payload).second) {
                duplicate.store(true);
            }
            if (free_node && free_node != payload)
                delete free_node;
            consumed.fetch_add(1, std::memory_order_relaxed);
        }
    });

    // Producer threads.
    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            for (int i = 0; i < kPerProducer; ++i) {
                // compute index using unsigned size_type to avoid signed->unsigned implicit conversion
                const std::size_t idx =
                    static_cast<std::size_t>(p) * static_cast<std::size_t>(kPerProducer) + static_cast<std::size_t>(i);
                q.push(all_nodes[idx]);
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& t : producers)
        t.join();

    // Let consumer finish.
    stop_consumer.store(true, std::memory_order_release);
    consumer.join();

    EXPECT_EQ(consumed.load(), kTotal);
    EXPECT_FALSE(duplicate.load()) << "Duplicate pointer received — queue is broken";

    for (auto* n : all_nodes)
        delete n;
}

TEST(VyukovMpscQueue, WakeupOnPush) {
    VyukovMpscQueue q;
    std::atomic<bool> stop{false};
    std::atomic<bool> woken{false};

    std::thread consumer([&] {
        q.wait_for_work(stop);
        woken.store(true, std::memory_order_release);
    });

    // Give the consumer time to enter wait_for_work.
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_FALSE(woken.load());

    MpscNode* n = make_node();
    q.push(n); // must wake the consumer

    consumer.join();
    EXPECT_TRUE(woken.load());

    // Drain leftover.
    auto [payload, free_node] = q.pop();
    if (free_node && free_node != payload)
        delete free_node;
    delete n;
}

TEST(VyukovMpscQueue, ForceWakeupUnblocks) {
    VyukovMpscQueue q;
    std::atomic<bool> stop{false};
    std::atomic<bool> returned{false};

    std::thread consumer([&] {
        q.wait_for_work(stop);
        returned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    q.force_wakeup();
    consumer.join();
    EXPECT_TRUE(returned.load());
}

TEST(VyukovMpscQueue, StopFlagUnblocks) {
    VyukovMpscQueue q;
    std::atomic<bool> stop{false};
    std::atomic<bool> returned{false};

    std::thread consumer([&] {
        q.wait_for_work(stop);
        returned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    stop.store(true, std::memory_order_release);
    q.force_wakeup(); // also poke the futex
    consumer.join();
    EXPECT_TRUE(returned.load());
}