#include "stratadb/wal/queue/spsc_mailbox_queue.hpp"
#include "stratadb/wal/queue/vyukov_mpsc_queue.hpp"
#include "stratadb/wal/types.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <gtest/gtest.h>
#include <memory>
#include <set>
#include <thread>
#include <vector>

using namespace stratadb::wal;

namespace {

using NodeOwner = std::unique_ptr<MpscNode>;
using NodeList = std::vector<NodeOwner>;

[[nodiscard]] auto make_node() -> NodeOwner {
    auto node = std::make_unique<MpscNode>();
    node->pool_managed = false;
    return node;
}

[[nodiscard]] auto make_nodes(std::size_t count) -> NodeList {
    NodeList nodes;
    nodes.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
        nodes.push_back(make_node());
    }
    return nodes;
}

void drain(VyukovMpscQueue& queue, std::vector<MpscNode*>& out) {
    while (true) {
        auto [payload, free_node] = queue.pop();
        (void)free_node;
        if (!payload) {
            break;
        }
        out.push_back(payload);
    }
}

} // namespace

TEST(VyukovMpscQueue, EmptyQueueReturnsNull) {
    VyukovMpscQueue queue;
    [[maybe_unused]] auto [payload, free_node] = queue.pop();
    EXPECT_EQ(payload, nullptr);
    EXPECT_EQ(free_node, nullptr);
}

TEST(VyukovMpscQueue, SingleThreadFIFO) {
    VyukovMpscQueue queue;

    constexpr std::size_t kItems = 100;
    auto nodes = make_nodes(kItems);

    for (const auto& node : nodes) {
        queue.push(node.get());
    }

    std::vector<MpscNode*> received;
    drain(queue, received);

    ASSERT_EQ(received.size(), kItems);

    for (std::size_t i = 0; i < kItems; ++i) {
        EXPECT_EQ(received[i], nodes[i].get()) << "order violation at index " << i;
    }
}

TEST(VyukovMpscQueue, MultiProducerStress) {
    VyukovMpscQueue queue;

    constexpr int kProducers = 16;
    constexpr int kPerProducer = 10000;
    constexpr int kTotal = kProducers * kPerProducer;

    auto all_nodes = make_nodes(static_cast<std::size_t>(kTotal));

    std::atomic<bool> start{false};
    std::atomic<int> consumed{0};
    std::atomic<bool> duplicate{false};
    std::atomic<bool> stop_consumer{false};
    std::set<MpscNode*> seen;

    std::thread consumer([&] -> void {
        while (!stop_consumer.load(std::memory_order_acquire) || consumed.load(std::memory_order_relaxed) < kTotal) {
            auto [payload, free_node] = queue.pop();
            (void)free_node;
            if (!payload) {
                std::this_thread::yield();
                continue;
            }
            if (!seen.insert(payload).second) {
                duplicate.store(true, std::memory_order_relaxed);
            }
            consumed.fetch_add(1, std::memory_order_relaxed);
        }
    });

    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int i = 0; i < kPerProducer; ++i) {
                const auto idx =
                    static_cast<std::size_t>(p) * static_cast<std::size_t>(kPerProducer) + static_cast<std::size_t>(i);
                queue.push(all_nodes[idx].get());
            }
        });
    }

    start.store(true, std::memory_order_release);
    for (auto& producer : producers) {
        producer.join();
    }

    stop_consumer.store(true, std::memory_order_release);
    consumer.join();

    EXPECT_EQ(consumed.load(), kTotal);
    EXPECT_FALSE(duplicate.load()) << "duplicate pointer received";
}

TEST(VyukovMpscQueue, WakeupOnPush) {
    VyukovMpscQueue queue;
    std::atomic<bool> stop{false};
    std::atomic<bool> woken{false};

    std::thread consumer([&] {
        queue.wait_for_work(stop);
        woken.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_FALSE(woken.load());

    auto node = make_node();
    queue.push(node.get());

    consumer.join();
    EXPECT_TRUE(woken.load());

    [[maybe_unused]] auto [payload, free_node] = queue.pop();
}

TEST(VyukovMpscQueue, ForceWakeupUnblocks) {
    VyukovMpscQueue queue;
    std::atomic<bool> stop{false};
    std::atomic<bool> returned{false};

    std::thread consumer([&] {
        queue.wait_for_work(stop);
        returned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    queue.force_wakeup();
    consumer.join();
    EXPECT_TRUE(returned.load());
}

TEST(VyukovMpscQueue, StopFlagUnblocks) {
    VyukovMpscQueue queue;
    std::atomic<bool> stop{false};
    std::atomic<bool> returned{false};

    std::thread consumer([&] {
        queue.wait_for_work(stop);
        returned.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    stop.store(true, std::memory_order_release);
    queue.force_wakeup();
    consumer.join();
    EXPECT_TRUE(returned.load());
}

TEST(SpscMailboxQueue, SingleProducerConsumerPingPong) {
    SpscMailboxQueue queue;

    constexpr std::size_t kItems = 100000;
    auto nodes = make_nodes(kItems);

    std::thread consumer([&] {
        std::size_t count = 0;
        while (count < kItems) {
            auto [payload, free_node] = queue.pop();
            (void)free_node;
            if (payload) {
                ++count;
            } else {
                std::this_thread::yield();
            }
        }
    });

    for (const auto& node : nodes) {
        queue.push(node.get());
    }

    consumer.join();
}

TEST(SpscMailboxQueue, MultipleMailboxesIsolated) {
    SpscMailboxQueue queue;

    constexpr std::size_t kProducers = 8;
    constexpr std::size_t kPerProducer = 500;

    std::vector<NodeList> nodes(kProducers);
    for (auto& producer_nodes : nodes) {
        producer_nodes = make_nodes(kPerProducer);
    }

    std::atomic<bool> start{false};
    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (std::size_t p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (const auto& node : nodes[p]) {
                queue.push(node.get());
            }
        });
    }

    start.store(true, std::memory_order_release);

    std::set<MpscNode*> seen;
    bool duplicate = false;
    std::size_t popped = 0;
    while (popped < kProducers * kPerProducer) {
        auto [payload, free_node] = queue.pop();
        (void)free_node;
        if (!payload) {
            std::this_thread::yield();
            continue;
        }
        if (!seen.insert(payload).second) {
            duplicate = true;
        }
        ++popped;
    }

    for (auto& producer : producers) {
        producer.join();
    }

    EXPECT_EQ(popped, kProducers * kPerProducer);
    EXPECT_FALSE(duplicate) << "duplicate node received";
}

TEST(SpscMailboxQueue, NoLossUnderConcurrentLoad) {
    SpscMailboxQueue queue;

    constexpr int kProducers = 4;
    constexpr int kPerProducer = 1000;
    constexpr int kTotal = kProducers * kPerProducer;

    auto all_nodes = make_nodes(static_cast<std::size_t>(kTotal));

    std::atomic<bool> start{false};
    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int i = 0; i < kPerProducer; ++i) {
                const auto idx =
                    static_cast<std::size_t>(p) * static_cast<std::size_t>(kPerProducer) + static_cast<std::size_t>(i);
                queue.push(all_nodes[idx].get());
            }
        });
    }

    start.store(true, std::memory_order_release);

    std::set<MpscNode*> seen;
    int popped = 0;
    while (popped < kTotal) {
        auto [payload, free_node] = queue.pop();
        (void)free_node;
        if (!payload) {
            std::this_thread::yield();
            continue;
        }
        seen.insert(payload);
        ++popped;
    }

    for (auto& producer : producers) {
        producer.join();
    }

    EXPECT_EQ(static_cast<int>(seen.size()), kTotal) << "items lost or duplicated";
}

TEST(SpscMailboxQueue, WaitAndWakeupNoOps) {
    SpscMailboxQueue queue;
    std::atomic<bool> stop{false};

    queue.wait_for_work(stop);
    queue.force_wakeup();
    SUCCEED();
}
