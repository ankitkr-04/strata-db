#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string_view>
namespace stratadb::memtable {

enum class ValueType : std::uint8_t { TypeDeletion = 0x00, TypeValue = 0x01 };

constexpr std::size_t SKIPLIST_NODE_HEADER_SIZE = 16;
constexpr std::size_t SKIPLIST_NODE_PREFIX_OFFSET = 9;
constexpr std::size_t SKIPLIST_NNODE_ALIGNMENT_STRUCT = 4;

struct SkipListNode {
    std::uint32_t key_len_;
    std::uint32_t val_len_;
    std::uint8_t height_;
    char prefix_[7];

    [[nodiscard]] auto next_nodes() noexcept -> std::atomic<SkipListNode*>*;
    [[nodiscard]] auto next_nodes() const noexcept -> const std::atomic<SkipListNode*>*;
    [[nodiscard]] auto internal_key() const noexcept -> std::string_view;
    [[nodiscard]] auto user_key() const noexcept -> std::string_view;

    [[nodiscard]] auto value() const noexcept -> std::string_view;
    [[nodiscard]] auto value_type() const noexcept -> ValueType;

    [[nodiscard]] auto sequence_number() const noexcept -> std::uint64_t;

    // Returns 0 on integer overflow (treated as OOM by the caller).
    [[nodiscard]] static auto
    allocation_size(std::uint8_t height, std::size_t user_key_len, std::size_t val_len) noexcept -> std::size_t;

    [[nodiscard]] static auto construct(void* mem,
                                        std::string_view key,
                                        std::string_view value,
                                        std::uint64_t sequence,
                                        ValueType type,
                                        std::uint8_t height) noexcept -> SkipListNode*;
};

// The header must be exactly 16 bytes so next_nodes() (which returns
// (this + 1)) lands on offset 16 — a natural 8-byte-aligned boundary
// required by std::atomic<SkipListNode*>.
static_assert(sizeof(SkipListNode) == SKIPLIST_NODE_HEADER_SIZE,
              "SkipListNode header must be exactly 16 bytes; check for unexpected padding");

// prefix_ must start at offset 9, filling the 7-byte alignment hole
// that would otherwise be dead padding before the 8-byte atomic array.
static_assert(offsetof(SkipListNode, prefix_) == SKIPLIST_NODE_PREFIX_OFFSET,
              "prefix_ must occupy offsets 9–15 (the 7-byte alignment hole)");

// Alignment of the struct itself is 4 (driven by uint32_t).
// Allocations must be requested with alignment 8 (for the atomic tower)
// — the allocator call-site is responsible for this.
static_assert(alignof(SkipListNode) == SKIPLIST_NNODE_ALIGNMENT_STRUCT,
              "SkipListNode header alignment is 4; callers must request 8-byte alignment");

} // namespace stratadb::memtable
