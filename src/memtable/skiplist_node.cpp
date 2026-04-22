#include "stratadb/memtable/skiplist_node.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <limits>
#include <new>

namespace stratadb::memtable {

namespace {
constexpr std::size_t MAX_SIZE = std::numeric_limits<std::size_t>::max();
constexpr std::size_t TRAILER_BYTES = 8;
constexpr std::size_t TYPE_BITS = 8;
constexpr std::uint64_t TYPE_MASK = 0xFF;
constexpr std::size_t PREFIX_BYTES = 7;

static_assert(TRAILER_BYTES == sizeof(std::uint64_t), "Trailer must be exactly 8 bytes");
} // namespace

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto SkipListNode::allocation_size(std::uint8_t height, std::size_t user_key_len, std::size_t val_len) noexcept
    -> std::size_t {

    const std::size_t tower_bytes = static_cast<std::size_t>(height) * sizeof(std::atomic<SkipListNode*>);

    if (user_key_len > MAX_SIZE - TRAILER_BYTES) [[unlikely]] {
        return 0; // Prevent overflow
    }

    const std::size_t ikey_bytes = user_key_len + TRAILER_BYTES;
    std::size_t total = sizeof(SkipListNode);

    if (tower_bytes > MAX_SIZE - total) [[unlikely]]
        return 0;
    total += tower_bytes;

    if (ikey_bytes > MAX_SIZE - total) [[unlikely]]
        return 0;
    total += ikey_bytes;

    if (val_len > MAX_SIZE - total) [[unlikely]]
        return 0;
    total += val_len;

    return total;
}

auto SkipListNode::next_nodes() noexcept -> std::atomic<SkipListNode*>* {
    return reinterpret_cast<std::atomic<SkipListNode*>*>(this + 1);
}

auto SkipListNode::next_nodes() const noexcept -> const std::atomic<SkipListNode*>* {
    return reinterpret_cast<const std::atomic<SkipListNode*>*>(this + 1);
}

static inline auto payload_start(const SkipListNode* n) noexcept -> const char* {
    return reinterpret_cast<const char*>(n->next_nodes() + n->height_);
}

auto SkipListNode::internal_key() const noexcept -> std::string_view {
    return {payload_start(this), key_len_};
}

auto SkipListNode::user_key() const noexcept -> std::string_view {
    assert(key_len_ >= TRAILER_BYTES);
    return {payload_start(this), key_len_ - TRAILER_BYTES};
}

auto SkipListNode::value() const noexcept -> std::string_view {
    const char* value_ptr = payload_start(this) + key_len_;
    return {value_ptr, val_len_};
}

static inline auto decode_trailer(const SkipListNode* n) noexcept -> std::uint64_t {
    assert(n->key_len_ >= TRAILER_BYTES);
    const char* trailer_ptr = payload_start(n) + (n->key_len_ - TRAILER_BYTES);
    std::uint64_t packed{};
    std::memcpy(&packed, trailer_ptr, sizeof(packed));
    return packed;
}

auto SkipListNode::sequence_number() const noexcept -> std::uint64_t {
    return decode_trailer(this) >> TYPE_BITS;
}

auto SkipListNode::value_type() const noexcept -> ValueType {
    return static_cast<ValueType>(decode_trailer(this) & TYPE_MASK);
}

auto SkipListNode::construct(void* mem,
                             std::string_view user_key,
                             std::string_view value,
                             std::uint64_t sequence,
                             ValueType type,
                             std::uint8_t height) noexcept -> SkipListNode* {
    assert(mem != nullptr);
    assert((reinterpret_cast<std::uintptr_t>(mem) % 8) == 0);
    assert(height >= 1);

    auto* node = static_cast<SkipListNode*>(mem);

    const auto ikey_len = static_cast<std::uint32_t>(user_key.size() + TRAILER_BYTES);
    node->key_len_ = ikey_len;
    node->val_len_ = static_cast<std::uint32_t>(value.size());
    node->height_ = height;

    // Zero-padded prefix copy
    const std::size_t copy_len = std::min(user_key.size(), PREFIX_BYTES);
    std::memcpy(node->prefix_, user_key.data(), copy_len);
    if (copy_len < PREFIX_BYTES) {
        std::memset(node->prefix_ + copy_len, '\0', PREFIX_BYTES - copy_len);
    }

    std::atomic<SkipListNode*>* tower = node->next_nodes();
    for (std::uint8_t i = 0; i < height; ++i) {
        new (tower + i) std::atomic<SkipListNode*>{nullptr};
    }

    char* payload = reinterpret_cast<char*>(tower + height);

    // Copy User Key
    std::memcpy(payload, user_key.data(), user_key.size());

    // Pack and Copy Trailer
    const std::uint64_t trailer = (sequence << TYPE_BITS) | static_cast<std::uint64_t>(type);
    std::memcpy(payload + user_key.size(), &trailer, sizeof(trailer));

    // Copy Value
    std::memcpy(payload + ikey_len, value.data(), value.size());

    return node;
}

} // namespace stratadb::memtable