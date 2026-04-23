
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/memtable/skiplist_memtable.hpp"
#include "stratadb/memtable/skiplist_node.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

using namespace stratadb::memtable;
using namespace stratadb::memory;
using namespace stratadb::config;

namespace {

constexpr std::size_t kArenaSize = 32ULL * 1024 * 1024;
constexpr std::size_t kTlabSize = 2ULL * 1024 * 1024;

[[nodiscard]] auto make_config(std::size_t total = kArenaSize) {
    MemoryConfig cfg{};
    cfg.total_budget_bytes = total;
    cfg.tlab_size_bytes = kTlabSize;
    cfg.page_strategy = PageStrategy::Standard4K;
    return cfg;
}

[[nodiscard]] auto make_arena() {
    return Arena::create(make_config()).value();
}

[[nodiscard]] auto make_tlab(Arena& arena) -> TLAB {
    // Adjust this constructor if your TLAB type is exposed differently.
    return TLAB{arena};
}
[[nodiscard]] auto make_memtable(Arena& arena) -> SkipListMemTable {
    return SkipListMemTable{arena};
}

[[nodiscard]] auto big_string(char ch, std::size_t n) -> std::string {
    return std::string(n, ch);
}

// ---------------- SkipListNode tests ----------------

TEST(SkipListNode, LayoutAndOffsets) {
    static_assert(sizeof(SkipListNode) == SKIPLIST_NODE_HEADER_SIZE);
    static_assert(offsetof(SkipListNode, prefix_) == SKIPLIST_NODE_PREFIX_OFFSET);
    static_assert(alignof(SkipListNode) == SKIPLIST_NNODE_ALIGNMENT_STRUCT);

    EXPECT_EQ(sizeof(SkipListNode), 16u);
    EXPECT_EQ(offsetof(SkipListNode, prefix_), 9u);
    EXPECT_EQ(alignof(SkipListNode), 4u);
}

TEST(SkipListNode, AllocationSizeOverflowGuards) {
    EXPECT_EQ(SkipListNode::allocation_size(0, 1, 1), 0u);
    EXPECT_GT(SkipListNode::allocation_size(1, 1, 1), 0u);
    EXPECT_EQ(SkipListNode::allocation_size(200, std::numeric_limits<std::size_t>::max(), 1), 0u);
}

TEST(SkipListNode, ConstructAndDecodeSmallKey) {
    const std::string key = "abc";
    const std::string value = "value";
    const std::uint64_t seq = 42;
    const ValueType type = ValueType::TypeValue;
    const std::uint8_t height = 3;

    const std::size_t sz = SkipListNode::allocation_size(height, key.size(), value.size());
    ASSERT_GT(sz, 0u);

    std::vector<std::byte> buf(sz + 8);
    void* mem = buf.data();
    std::size_t space = buf.size();

    void* aligned = std::align(8, sz, mem, space);
    ASSERT_NE(aligned, nullptr);

    mem = aligned;

    auto* node = SkipListNode::construct(mem, key, value, seq, type, height);
    ASSERT_NE(node, nullptr);

    EXPECT_EQ(node->user_key(), key);
    EXPECT_EQ(node->internal_key().substr(0, key.size()), key);
    EXPECT_EQ(node->value(), value);
    EXPECT_EQ(node->value_type(), type);
    EXPECT_EQ(node->sequence_number(), seq);
    EXPECT_EQ(node->height_, height);
    EXPECT_EQ(node->key_len_, key.size() + 8);
    EXPECT_EQ(node->val_len_, value.size());
}

TEST(SkipListNode, ConstructAndDecodeLongKeyPrefix) {
    const std::string key = "abcdefghijklmno";
    const std::string value = "x";

    const std::size_t sz = SkipListNode::allocation_size(1, key.size(), value.size());
    ASSERT_GT(sz, 0u);

    std::vector<std::byte> buf(sz + 8);
    void* mem = buf.data();
    std::size_t space = buf.size();

    void* aligned = std::align(8, sz, mem, space);
    ASSERT_NE(aligned, nullptr);

    mem = aligned;

    auto* node = SkipListNode::construct(mem, key, value, 7, ValueType::TypeDeletion, 1);
    ASSERT_NE(node, nullptr);

    EXPECT_EQ(node->user_key(), key);
    EXPECT_EQ(node->sequence_number(), 7u);
    EXPECT_EQ(node->value_type(), ValueType::TypeDeletion);
    EXPECT_EQ(std::memcmp(node->prefix_, key.data(), 7), 0);
}

TEST(SkipListNode, NextNodesAreInitializedToNull) {
    const std::string key = "node";
    const std::string value = "v";
    const std::uint8_t height = 4;

    const std::size_t sz = SkipListNode::allocation_size(height, key.size(), value.size());
    ASSERT_GT(sz, 0u);

    std::vector<std::byte> buf(sz + 8);
    void* mem = buf.data();
    std::size_t space = buf.size();

    void* aligned = std::align(8, sz, mem, space);
    ASSERT_NE(aligned, nullptr);

    mem = aligned;

    auto* node = SkipListNode::construct(mem, key, value, 99, ValueType::TypeValue, height);
    ASSERT_NE(node, nullptr);

    auto* tower = node->next_nodes();
    for (std::uint8_t i = 0; i < height; ++i) {
        EXPECT_EQ(tower[i].load(std::memory_order_relaxed), nullptr);
    }
}

// ---------------- SkipListMemTable tests ----------------

TEST(SkipListMemTable, ConceptSatisfied) {
    static_assert(IsMemTable<SkipListMemTable>);
    SUCCEED();
}

TEST(SkipListMemTable, EmptyGetReturnsNullopt) {
    auto arena = make_arena();
    auto memtable = make_memtable(arena);

    EXPECT_FALSE(memtable.get("missing").has_value());
    EXPECT_EQ(memtable.memory_usage(), 0u);
}

TEST(SkipListMemTable, PutGetAndOverwriteLatestWins) {
    auto arena = make_arena();
    auto memtable = make_memtable(arena);
    auto tlab = make_tlab(arena);

    ASSERT_TRUE(memtable.put("k1", "v1", tlab, std::numeric_limits<std::size_t>::max()));
    ASSERT_TRUE(memtable.put("k1", "v2", tlab, std::numeric_limits<std::size_t>::max()));

    auto got = memtable.get("k1");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "v2");
    EXPECT_GT(memtable.memory_usage(), 0u);
}

TEST(SkipListMemTable, RemoveCreatesTombstone) {
    auto arena = make_arena();
    auto memtable = make_memtable(arena);
    auto tlab = make_tlab(arena);

    ASSERT_TRUE(memtable.put("dead", "alive", tlab, std::numeric_limits<std::size_t>::max()));
    ASSERT_TRUE(memtable.remove("dead", tlab));

    EXPECT_FALSE(memtable.get("dead").has_value());
}

TEST(SkipListMemTable, FlushThresholdBlocksWrites) {
    auto arena = make_arena();
    auto memtable = make_memtable(arena);
    auto tlab = make_tlab(arena);

    EXPECT_TRUE(memtable.should_flush(0));
    EXPECT_FALSE(memtable.put("blocked", "v", tlab, 0));
}

TEST(SkipListMemTable, MemoryUsageTracksSuccessfulInserts) {
    auto arena = make_arena();
    auto memtable = make_memtable(arena);
    auto tlab = make_tlab(arena);

    const auto before = memtable.memory_usage();
    ASSERT_TRUE(memtable.put("a", "1", tlab, std::numeric_limits<std::size_t>::max()));
    ASSERT_TRUE(memtable.put("b", "2", tlab, std::numeric_limits<std::size_t>::max()));
    const auto after = memtable.memory_usage();

    EXPECT_GT(after, before);
}

TEST(SkipListMemTable, LongKeyRoundTrip) {
    auto arena = make_arena();
    auto memtable = make_memtable(arena);
    auto tlab = make_tlab(arena);

    const std::string key = big_string('k', 64);
    const std::string value = big_string('v', 128);

    ASSERT_TRUE(memtable.put(key, value, tlab, std::numeric_limits<std::size_t>::max()));

    auto got = memtable.get(key);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, value);
}

TEST(SkipListMemTable, LargeBatchRoundTrip) {
    auto arena = make_arena();
    auto memtable = make_memtable(arena);
    auto tlab = make_tlab(arena);

    constexpr std::size_t n = 512;
    for (std::size_t i = 0; i < n; ++i) {
        const auto key = "key_" + std::to_string(i);
        const auto value = "val_" + std::to_string(i);
        ASSERT_TRUE(memtable.put(key, value, tlab, std::numeric_limits<std::size_t>::max()));
    }

    for (std::size_t i = 0; i < n; ++i) {
        const auto key = "key_" + std::to_string(i);
        const auto value = "val_" + std::to_string(i);
        auto got = memtable.get(key);
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(*got, value);
    }
}

TEST(SkipListMemTable, ConcurrentUniqueInserts) {
    auto arena = make_arena();
    auto memtable = make_memtable(arena);

    constexpr std::size_t threads = 4;
    constexpr std::size_t per_thread = 128;

    std::array<std::thread, threads> workers{};
    for (std::size_t t = 0; t < threads; ++t) {
        workers[t] = std::thread([&, t] {
            auto tlab = make_tlab(arena);
            for (std::size_t i = 0; i < per_thread; ++i) {
                const auto key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                const auto value = "v" + std::to_string(i);
                ASSERT_TRUE(memtable.put(key, value, tlab, std::numeric_limits<std::size_t>::max()));
            }
        });
    }

    for (auto& th : workers) {
        th.join();
    }

    for (std::size_t t = 0; t < threads; ++t) {
        for (std::size_t i = 0; i < per_thread; ++i) {
            const auto key = "t" + std::to_string(t) + "_k" + std::to_string(i);
            const auto value = "v" + std::to_string(i);
            auto got = memtable.get(key);
            ASSERT_TRUE(got.has_value());
            EXPECT_EQ(*got, value);
        }
    }
}

TEST(SkipListMemTable, ConcurrentSameKeyLastWriterVisible) {
    auto arena = make_arena();
    auto memtable = make_memtable(arena);

    constexpr std::size_t threads = 8;
    std::array<std::thread, threads> workers{};

    for (std::size_t t = 0; t < threads; ++t) {
        workers[t] = std::thread([&, t] {
            auto tlab = make_tlab(arena);
            const auto value = "v" + std::to_string(t);
            ASSERT_TRUE(memtable.put("shared", value, tlab, std::numeric_limits<std::size_t>::max()));
        });
    }

    for (auto& th : workers) {
        th.join();
    }

    auto got = memtable.get("shared");
    ASSERT_TRUE(got.has_value());
    EXPECT_FALSE(got->empty());
}

TEST(SkipListMemTable, RejectsOverflowSizedPayloads) {
    EXPECT_EQ(SkipListNode::allocation_size(1, std::numeric_limits<std::size_t>::max(), 1), 0u);
    EXPECT_EQ(SkipListNode::allocation_size(1, 1, std::numeric_limits<std::size_t>::max()), 0u);
}
} // namespace