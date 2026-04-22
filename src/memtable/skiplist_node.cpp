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

} // namespace stratadb::memtable