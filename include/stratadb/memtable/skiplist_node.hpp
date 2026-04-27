#pragma once

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <string_view>

namespace stratadb::memtable {

enum class ValueType : std::uint8_t { TypeDeletion = 0x00, TypeValue = 0x01 };

struct SkipListNode {
    static constexpr std::size_t PREFIX_BYTES = 7;
    static constexpr std::size_t TRAILER_BYTES = 8;
    static constexpr std::size_t TYPE_BITS = 8;
    static constexpr std::uint64_t TYPE_MASK = (std::uint64_t{1} << TYPE_BITS) - 1;
    static constexpr std::uint64_t MAX_SEQUENCE = std::numeric_limits<std::uint64_t>::max() >> TYPE_BITS;
    static constexpr std::size_t REQUIRED_ALIGNMENT = alignof(std::atomic<SkipListNode*>);

    std::uint32_t key_len_{};
    std::uint32_t val_len_{};
    std::uint8_t height_{};
    std::array<unsigned char, PREFIX_BYTES> prefix_{};

    [[nodiscard]] static constexpr auto header_size() noexcept -> std::size_t {
        return sizeof(SkipListNode);
    }

    [[nodiscard]] static constexpr auto prefix_offset() noexcept -> std::size_t {
        return offsetof(SkipListNode, prefix_);
    }

    [[nodiscard]] static constexpr auto struct_alignment() noexcept -> std::size_t {
        return alignof(SkipListNode);
    }

    [[nodiscard]] auto next_nodes() noexcept -> std::span<std::atomic<SkipListNode*>> {
        return {tower_ptr(), height_};
    }

    [[nodiscard]] auto next_nodes() const noexcept -> std::span<const std::atomic<SkipListNode*>> {
        return {tower_ptr(), height_};
    }

    [[nodiscard]] auto internal_key() const noexcept -> std::string_view {
        return {payload_ptr(), key_len_};
    }

    [[nodiscard]] auto user_key() const noexcept -> std::string_view {
        assert(key_len_ >= TRAILER_BYTES);
        return {payload_ptr(), key_len_ - TRAILER_BYTES};
    }

    [[nodiscard]] auto value() const noexcept -> std::string_view {
        const char* value_ptr = payload_ptr() + key_len_;
        return {value_ptr, val_len_};
    }

    [[nodiscard]] auto value_type() const noexcept -> ValueType {
        return static_cast<ValueType>(unpack_trailer() & TYPE_MASK);
    }

    [[nodiscard]] auto sequence_number() const noexcept -> std::uint64_t {
        return unpack_trailer() >> TYPE_BITS;
    }

    [[nodiscard]] auto is_tombstone() const noexcept -> bool {
        return value_type() == ValueType::TypeDeletion;
    }

    // Returns 0 on integer overflow (treated as OOM by the caller).
    [[nodiscard]] static auto
    allocation_size(std::uint8_t height, std::size_t user_key_len, std::size_t val_len) noexcept -> std::size_t;

    static auto construct(void* mem,
                          std::string_view key,
                          std::string_view value,
                          std::uint64_t sequence,
                          ValueType type,
                          std::uint8_t height) noexcept -> SkipListNode&;

  private:
    [[nodiscard]] auto tower_ptr() noexcept -> std::atomic<SkipListNode*>* {
        return reinterpret_cast<std::atomic<SkipListNode*>*>(this + 1);
    }

    [[nodiscard]] auto tower_ptr() const noexcept -> const std::atomic<SkipListNode*>* {
        return reinterpret_cast<const std::atomic<SkipListNode*>*>(this + 1);
    }

    [[nodiscard]] auto payload_ptr() const noexcept -> const char* {
        return reinterpret_cast<const char*>(tower_ptr() + height_);
    }

    [[nodiscard]] auto unpack_trailer() const noexcept -> std::uint64_t {
        assert(key_len_ >= TRAILER_BYTES);
        const char* trailer_ptr = payload_ptr() + (key_len_ - TRAILER_BYTES);
        std::uint64_t packed{};
        std::memcpy(&packed, trailer_ptr, sizeof(packed));
        return packed;
    }
};

// The header must be exactly 16 bytes so next_nodes() (which returns
// (this + 1)) lands on offset 16 — a natural 8-byte-aligned boundary
// required by std::atomic<SkipListNode*>.
static_assert(sizeof(SkipListNode) == SkipListNode::header_size(),
              "SkipListNode header must be exactly 16 bytes; check for unexpected padding");

// prefix_ must start at offset 9, filling the 7-byte alignment hole
// that would otherwise be dead padding before the 8-byte atomic array.
static_assert(offsetof(SkipListNode, prefix_) == SkipListNode::prefix_offset(),
              "prefix_ must occupy offsets 9–15 (the 7-byte alignment hole)");

// Alignment of the struct itself is 4 (driven by uint32_t).
// Allocations must be requested with alignment 8 (for the atomic tower)
// — the allocator call-site is responsible for this.
static_assert(alignof(SkipListNode) == SkipListNode::struct_alignment(),
              "SkipListNode header alignment is 4; callers must request 8-byte alignment");

static_assert(static_cast<std::uint64_t>(ValueType::TypeDeletion) <= SkipListNode::TYPE_MASK,
              "ValueType::TypeDeletion must fit in trailer type bits");
static_assert(static_cast<std::uint64_t>(ValueType::TypeValue) <= SkipListNode::TYPE_MASK,
              "ValueType::TypeValue must fit in trailer type bits");

} // namespace stratadb::memtable
