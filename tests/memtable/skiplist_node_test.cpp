#include "stratadb/memtable/skiplist_node.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <gtest/gtest.h>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

using stratadb::memtable::SkipListNode;
using stratadb::memtable::ValueType;

namespace {

struct NodeBuffer {
    std::vector<std::byte> storage;
    SkipListNode* node{nullptr};
};

[[nodiscard]] auto
make_node(std::string_view key, std::string_view value, std::uint64_t sequence, ValueType type, std::uint8_t height)
    -> NodeBuffer {
    const std::size_t size = SkipListNode::allocation_size(height, key.size(), value.size());
    EXPECT_GT(size, 0u);

    std::vector<std::byte> storage(size + 8);
    void* mem = storage.data();
    std::size_t space = storage.size();

    void* aligned = std::align(8, size, mem, space);
    EXPECT_NE(aligned, nullptr);

    auto& node = SkipListNode::construct(aligned, key, value, sequence, type, height);
    return {.storage = std::move(storage), .node = &node};
}

} // namespace

TEST(SkipListNode, LayoutAndOffsets) {
    static_assert(sizeof(SkipListNode) == SkipListNode::header_size());
    static_assert(offsetof(SkipListNode, prefix_) == SkipListNode::prefix_offset());
    static_assert(alignof(SkipListNode) == SkipListNode::struct_alignment());

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
    const std::uint64_t sequence = 42;
    const ValueType type = ValueType::TypeValue;
    const std::uint8_t height = 3;

    auto buffer = make_node(key, value, sequence, type, height);
    const auto* node = buffer.node;

    EXPECT_EQ(node->user_key(), key);
    EXPECT_EQ(node->internal_key().substr(0, key.size()), key);
    EXPECT_EQ(node->value(), value);
    EXPECT_EQ(node->value_type(), type);
    EXPECT_EQ(node->sequence_number(), sequence);
    EXPECT_EQ(node->height_, height);
    EXPECT_EQ(node->key_len_, key.size() + 8);
    EXPECT_EQ(node->val_len_, value.size());
}

TEST(SkipListNode, ConstructAndDecodeLongKeyPrefix) {
    const std::string key = "abcdefghijklmno";

    auto buffer = make_node(key, "x", 7, ValueType::TypeDeletion, 1);
    const auto* node = buffer.node;

    EXPECT_EQ(node->user_key(), key);
    EXPECT_EQ(node->sequence_number(), 7u);
    EXPECT_EQ(node->value_type(), ValueType::TypeDeletion);
    EXPECT_EQ(std::memcmp(node->prefix_.data(), key.data(), SkipListNode::PREFIX_BYTES), 0);
}

TEST(SkipListNode, NextNodesAreInitializedToNull) {
    constexpr std::uint8_t kHeight = 4;

    auto buffer = make_node("node", "v", 99, ValueType::TypeValue, kHeight);
    const auto* node = buffer.node;

    auto tower = node->next_nodes();
    for (std::uint8_t i = 0; i < kHeight; ++i) {
        EXPECT_EQ(tower[i].load(std::memory_order_relaxed), nullptr);
    }
}

TEST(SkipListNode, MaxSequenceFitsInTrailerBits) {
    const std::uint64_t shifted = SkipListNode::MAX_SEQUENCE << SkipListNode::TYPE_BITS;
    EXPECT_GT(shifted, 0u);
    EXPECT_LE(SkipListNode::MAX_SEQUENCE, std::numeric_limits<std::uint64_t>::max() >> SkipListNode::TYPE_BITS);
}

TEST(SkipListNode, AllocationSizeZeroHeightIsZero) {
    EXPECT_EQ(SkipListNode::allocation_size(0, 128, 256), 0u);
}

TEST(SkipListNode, AllocationSizeMonotonicallyIncreasesWithHeight) {
    std::size_t previous = 0;
    for (std::uint8_t height = 1; height <= 12; ++height) {
        const std::size_t size = SkipListNode::allocation_size(height, 16, 64);
        EXPECT_GT(size, previous) << "allocation_size did not grow at height " << static_cast<int>(height);
        previous = size;
    }
}
