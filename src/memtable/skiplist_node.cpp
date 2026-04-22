#include "stratadb/memtable/skiplist_node.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <limits>
#include <new>

namespace stratadb::memtable {
constexpr std::size_t k_max = std::numeric_limits<std::size_t>::max();
constexpr std::size_t trailerbytes = 8; // 8 bytes for sequence number and value type

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto SkipListNode::allocation_size(std::uint8_t height, std::size_t key_len, std::size_t val_len) noexcept
    -> std::size_t {

    // heignt is atmost 12, so height * 8 = 96, no overflow
    const std::size_t tower_bytes = static_cast<std::size_t>(height) * sizeof(std::atomic<SkipListNode*>);

    if (key_len > k_max - trailerbytes) [[unlikely]] {
        return 0;
    }

    const std::size_t ikey_bytes = key_len + trailerbytes;

    std::size_t total = sizeof(SkipListNode);

    // Add tower.
    if (tower_bytes > k_max - total) [[unlikely]] {
        return 0;
    }
    total += tower_bytes;

    // Add InternalKey.
    if (ikey_bytes > k_max - total) [[unlikely]] {
        return 0;
    }
    total += ikey_bytes;

    // Add value.
    if (val_len > k_max - total) [[unlikely]] {
        return 0;
    }
    total += val_len;

    return total;
}

auto SkipListNode::next_nodes() noexcept -> std::atomic<SkipListNode*>* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return reinterpret_cast<std::atomic<SkipListNode*>*>(this + 1);
}

auto SkipListNode::next_nodes() const noexcept -> const std::atomic<SkipListNode*>* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return reinterpret_cast<const std::atomic<SkipListNode*>*>(this + 1);
}

static inline auto payload_start(const SkipListNode* n) noexcept -> const char* {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return reinterpret_cast<const char*>(n->next_nodes() + n->height_);
}

// ── internal_key
auto SkipListNode::internal_key() const noexcept -> std::string_view {
    return {payload_start(this), key_len_};
}

// ── user_key
auto SkipListNode::user_key() const noexcept -> std::string_view {
    // Strip the 8-byte trailer from key_len_.
    assert(key_len_ >= 8 && "InternalKey must be at least 8 bytes (trailer only)");
    return {payload_start(this), key_len_ - 8};
}

static inline auto decode_trailer(const SkipListNode* n) noexcept -> std::uint64_t {
    assert(n->key_len_ >= 8);
    const char* trailer_ptr = payload_start(n) + (n->key_len_ - 8);
    std::uint64_t packed{};
    std::memcpy(&packed, trailer_ptr, sizeof(packed));
    return packed;
}

auto SkipListNode::sequence_number() const noexcept -> std::uint64_t {
    return decode_trailer(this) >> trailerbytes; // upper 56 bits
}

// ── value_type
auto SkipListNode::value_type() const noexcept -> ValueType {
    return static_cast<ValueType>(decode_trailer(this) & 0xFF); // lowest byte
}

// ── value
auto SkipListNode::value() const noexcept -> std::string_view {
    // Value bytes start immediately after the InternalKey region.
    const char* value_ptr = payload_start(this) + key_len_;
    return {value_ptr, val_len_};
}

auto SkipListNode::construct(void* mem,
                             // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                             std::string_view user_key,
                             std::string_view value,
                             std::uint64_t sequence,
                             ValueType type,
                             std::uint8_t height) noexcept -> SkipListNode* {
    assert(mem != nullptr);
    assert((reinterpret_cast<std::uintptr_t>(mem) % 8) == 0
           && "TLAB allocation must be 8-byte aligned for atomic<SkipListNode*>");
    assert(height >= 1);
    assert(sequence <= ((std::uint64_t{1} << 56) - 1)
           && "Sequence number must fit in 56 bits (trailer = (seq<<8)|type)");

    auto* node = static_cast<SkipListNode*>(mem);

    const auto ikey_len = static_cast<std::uint32_t>(user_key.size() + trailerbytes);
    node->key_len_ = ikey_len;
    node->val_len_ = static_cast<std::uint32_t>(value.size());
    node->height_ = height;

    const std::size_t copy_len = std::min(user_key.size(), std::size_t{7});

    if (copy_len < 7) {
        std::memset(node->prefix_ + copy_len, '\0', 7 - copy_len);
    }

    std::atomic<SkipListNode*>* tower = node->next_nodes();

    for (std::uint8_t i = 0; i < height; ++i) {
        new (tower + i) std::atomic<SkipListNode*>{nullptr};
    }

    char* payload = reinterpret_cast<char*>(tower + height);

    std::memcpy(payload, user_key.data(), user_key.size());

    const std::uint64_t trailer = (sequence << trailerbytes) | static_cast<std::uint64_t>(type);

    std::memcpy(payload + user_key.size(), &trailer, sizeof(trailer));

    std::memcpy(payload + ikey_len, value.data(), value.size());

    return node;
}

} // namespace stratadb::memtable