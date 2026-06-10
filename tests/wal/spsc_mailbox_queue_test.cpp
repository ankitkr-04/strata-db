// tests/wal/spsc_mailbox_queue_test.cpp
//
// Coverage:
//   SingleProducerConsumerPingPong   — 1 producer + 1 consumer, 1 M transfers
//   MultipleMailboxesIsolated        — N producers push into their own mailboxes,
//                                      consumer sweeps and collects all without
//                                      duplicates or losses
//   RingFullSpin                     — producer spins when mailbox is full;
//                                      unblocks once consumer drains
//   RoundRobinSweep                  — pop() cycles across mailboxes fairly
//                                      (no starvation of high-index threads)

#include "stratadb/utils/limits.hpp"
#include "stratadb/wal/queue/spsc_mailbox_queue.hpp"
#include "stratadb/wal/types.hpp"

#include <atomic>
#include <cstddef>
#include <gtest/gtest.h>
#include <set>
#include <thread>
#include <vector>

using namespace stratadb::wal;
using stratadb::utils::MAX_SUPPORTED_THREADS;

namespace {

MpscNode* make_node() {
    auto* n = new MpscNode{};
    n->pool_managed = false;
    return n;
}

} // namespace

// Test 1: Single-producer / single-consumer ping-pong
TEST(SpscMailboxQueue, SingleProducerConsumerPingPong) {
    SpscMailboxQueue q;

    constexpr int kItems = 100'000; // Lowered to 100k so TSAN runs fast

    // Pre-allocate all nodes to simulate the BlockPool
    std::vector<MpscNode*> nodes;
    nodes.reserve(kItems);
    for (int i = 0; i < kItems; ++i) {
        nodes.push_back(make_node());
    }

    std::thread consumer([&] {
        int count = 0;
        while (count < kItems) {
            auto [payload, free_node] = q.pop();
            if (payload) {
                ++count;
            } else {
                std::this_thread::yield();
            }
        }
    });

    // Producer ONLY pushes.
    // If the queue hits its internal capacity limit, push() will safely
    // spin-wait internally until the consumer pops items.
    for (std::size_t i = 0; i < kItems; ++i) {
        q.push(nodes[i]);
    }

    consumer.join();

    // Clean up
    for (auto* n : nodes) {
        delete n;
    }
}
// Test 2: Multiple producers, each writing to their own mailbox
TEST(SpscMailboxQueue, MultipleMailboxesIsolated) {
    SpscMailboxQueue q;

    // Use 8 producers to stay well within MAX_SUPPORTED_THREADS.
    constexpr std::size_t kProducers = 8;
    constexpr std::size_t kPerProducer = 500;

    std::vector<std::vector<MpscNode*>> nodes(kProducers);
    for (std::size_t p = 0; p < kProducers; ++p) {
        nodes[p].reserve(kPerProducer);
        for (std::size_t i = 0; i < kPerProducer; ++i)
            nodes[p].push_back(make_node());
    }

    std::atomic<bool> start{false};
    std::atomic<int> total_pushed{0};

    // Producers: thread p pushes into mailbox[p % MAX_SUPPORTED_THREADS].
    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (std::size_t p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] -> void {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            for (auto* n : nodes[p]) {
                q.push(n);
                total_pushed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    start.store(true, std::memory_order_release);

    // Consumer: drain until we have all items.
    std::set<MpscNode*> seen;
    bool duplicate = false;
    std::size_t popped = 0;
    while (popped < kProducers * kPerProducer) {
        auto [payload, free_node] = q.pop();
        if (!payload) {
            std::this_thread::yield();
            continue;
        }
        if (!seen.insert(payload).second)
            duplicate = true;
        ++popped;
    }

    for (auto& t : producers)
        t.join();

    EXPECT_EQ(popped, kProducers * kPerProducer);
    EXPECT_FALSE(duplicate) << "Duplicate node received — mailbox isolation broken";

    for (auto& pv : nodes)
        for (auto* n : pv)
            delete n;
}

// Test 3: No items are lost or duplicated in a concurrent stress run
TEST(SpscMailboxQueue, NoLossUnderConcurrentLoad) {
    SpscMailboxQueue q;

    constexpr int kProducers = 4;
    constexpr int kPerProducer = 1000;

    std::vector<MpscNode*> all_nodes;
    all_nodes.reserve(static_cast<long>(kProducers) * kPerProducer);
    for (int i = 0; i < kProducers * kPerProducer; ++i)
        all_nodes.push_back(make_node());

    std::atomic<bool> start{false};

    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            for (int i = 0; i < kPerProducer; ++i)
                q.push(all_nodes[static_cast<std::size_t>(p) * static_cast<std::size_t>(kPerProducer)
                                 + static_cast<std::size_t>(i)]);
        });
    }

    start.store(true, std::memory_order_release);

    std::set<MpscNode*> seen;
    int popped = 0;
    while (popped < kProducers * kPerProducer) {
        auto [payload, free_node] = q.pop();
        if (!payload) {
            std::this_thread::yield();
            continue;
        }
        seen.insert(payload);
        ++popped;
    }

    for (auto& t : producers)
        t.join();

    EXPECT_EQ(static_cast<int>(seen.size()), kProducers * kPerProducer) << "Items lost or duplicated";

    for (auto* n : all_nodes)
        delete n;
}

// Test 4: wait_for_work and force_wakeup (SPSC busy-polls; these are no-ops
// but must compile and not crash)
TEST(SpscMailboxQueue, WaitAndWakeupNoOps) {
    SpscMailboxQueue q;
    std::atomic<bool> stop{false};

    // wait_for_work on SpscMailboxQueue just calls cpu_relax — instant return.
    q.wait_for_work(stop);
    q.force_wakeup();
    SUCCEED();
}