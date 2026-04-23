#include "stratadb/memtable/skiplist_node.hpp"

#include <cassert>
#include <limits>
#include <new>

namespace stratadb::memtable {

namespace {
constexpr std::size_t MAX_SIZE = std::numeric_limits<std::size_t>::max();
constexpr std::uint32_t MAX_U32 = std::numeric_limits<std::uint32_t>::max();

static_assert(SkipListNode::TRAILER_BYTES == sizeof(std::uint64_t), "Trailer must be exactly 8 bytes");
} // namespace

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
auto SkipListNode::allocation_size(std::uint8_t height, std::size_t user_key_len, std::size_t val_len) noexcept

    -> std::size_t {

    if (height == 0) [[unlikely]] {
        return 0;
    }

    const std::size_t tower_bytes = static_cast<std::size_t>(height) * sizeof(std::atomic<SkipListNode*>);

    if (user_key_len > MAX_SIZE - TRAILER_BYTES) [[unlikely]] {
        return 0; // Prevent overflow
    }

    if (user_key_len > static_cast<std::size_t>(MAX_U32) - TRAILER_BYTES) [[unlikely]] {
        return 0;
    }

    if (val_len > MAX_U32) [[unlikely]] {
        return 0;
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

auto SkipListNode::construct(void* mem,
                             std::string_view user_key,
                             std::string_view value,
                             std::uint64_t sequence,
                             ValueType type,
                             std::uint8_t height) noexcept -> SkipListNode& {
    assert(mem != nullptr);
    assert((reinterpret_cast<std::uintptr_t>(mem) % REQUIRED_ALIGNMENT) == 0);
    assert(height >= 1);
    assert(user_key.size() <= static_cast<std::size_t>(MAX_U32) - TRAILER_BYTES);
    assert(value.size() <= static_cast<std::size_t>(MAX_U32));
    assert(sequence <= MAX_SEQUENCE);

    auto* node = static_cast<SkipListNode*>(mem);

    const auto ikey_len = static_cast<std::uint32_t>(user_key.size() + TRAILER_BYTES);
    node->key_len_ = ikey_len;
    node->val_len_ = static_cast<std::uint32_t>(value.size());
    node->height_ = height;

    // Zero-padded prefix copy
    const std::size_t copy_len = std::min(user_key.size(), PREFIX_BYTES);
    std::memcpy(node->prefix_.data(), user_key.data(), copy_len);
    if (copy_len < PREFIX_BYTES) {
        std::memset(node->prefix_.data() + copy_len, '\0', PREFIX_BYTES - copy_len);
    }

    auto tower = node->next_nodes();
    for (std::uint8_t i = 0; i < height; ++i) {
        new (&tower[i]) std::atomic<SkipListNode*>{nullptr};
    }

    char* payload = reinterpret_cast<char*>(tower.data() + height);

    // Copy User Key
    std::memcpy(payload, user_key.data(), user_key.size());

    // Pack and Copy Trailer
    const std::uint64_t trailer = (sequence << TYPE_BITS) | static_cast<std::uint64_t>(type);
    std::memcpy(payload + user_key.size(), &trailer, sizeof(trailer));

    // Copy Value
    std::memcpy(payload + ikey_len, value.data(), value.size());

    return *node;
}

} // namespace stratadb::memtable