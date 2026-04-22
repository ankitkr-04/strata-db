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

} // namespace stratadb::memtable