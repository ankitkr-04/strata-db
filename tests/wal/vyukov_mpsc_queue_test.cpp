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

MpscNode* make_node() {
    auto* n = new MpscNode{};
    n->pool_managed = false;
    return n;
}

// Pop all available items from the queue into `out`.
void drain(VyukovMpscQueue& q, std::vector<MpscNode*>& out) {
    while (true) {
        auto [payload, free_node] = q.pop();
        if (!payload)
            break;
        out.push_back(payload);

        // FIX: Do NOT delete free_node here. The first free_node is the
        // statically allocated `stub_`. The test will clean up all allocated
        // nodes using the tracking vectors at the end of the test.
    }
}

} // namespace

// Tests

TEST(VyukovMpscQueue, EmptyQueueReturnsNull) {
    VyukovMpscQueue q;
    [[maybe_unused]] auto [payload, free_node] = q.pop();
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

    for (std::size_t i = 0; i < N; ++i) {
        EXPECT_EQ(received[i], nodes[i]) << "Order violation at index " << i;
    }

    // RESTORED FIX: Safely delete all nodes we manually allocated.
    for (auto* n : nodes)
        delete n;
}

TEST(VyukovMpscQueue, MultiProducerStress) {
    VyukovMpscQueue q;

    constexpr int kProducers = 16;
    constexpr int kPerProducer = 10'000;
    constexpr int kTotal = kProducers * kPerProducer;

    std::vector<MpscNode*> all_nodes;
    all_nodes.reserve(kTotal);
    for (int i = 0; i < kTotal; ++i)
        all_nodes.push_back(make_node());

    std::atomic<bool> start{false};

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
            // FIX: Removed the unsafe delete free_node
            consumed.fetch_add(1, std::memory_order_relaxed);
        }
    });

    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            while (!start.load(std::memory_order_acquire))
                std::this_thread::yield();
            for (int i = 0; i < kPerProducer; ++i) {
                const std::size_t idx =
                    static_cast<std::size_t>(p) * static_cast<std::size_t>(kPerProducer) + static_cast<std::size_t>(i);
                q.push(all_nodes[idx]);
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& t : producers)
        t.join();

    stop_consumer.store(true, std::memory_order_release);
    consumer.join();

    EXPECT_EQ(consumed.load(), kTotal);
    EXPECT_FALSE(duplicate.load()) << "Duplicate pointer received — queue is broken";

    // RESTORED FIX: Safely clean up all memory.
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

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_FALSE(woken.load());

    MpscNode* n = make_node();
    q.push(n);

    consumer.join();
    EXPECT_TRUE(woken.load());

    [[maybe_unused]] auto [payload, free_node] = q.pop();

    // RESTORED FIX
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
    q.force_wakeup();
    consumer.join();
    EXPECT_TRUE(returned.load());
}