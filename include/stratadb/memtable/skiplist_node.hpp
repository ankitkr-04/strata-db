#pragma once
#include <atomic>
#include <cstdint>
#include <string_view>
namespace stratadb::memtable {

enum class ValueType : std::uint8_t { TypeDeletion = 0x00, TypeValue = 0x01 };

struct SkipListNode {
    std::uint32_t key_len_;
    std::uint32_t val_len_;
    std::uint8_t height_;
    char prefix_[7];

    [[nodiscard]] auto next_nodes() noexcept -> std::atomic<SkipListNode*>*;
    [[nodiscard]] auto next_nodes() const noexcept -> const std::atomic<SkipListNode*>*;
    [[nodiscard]] auto internal_key() const noexcept -> std::string_view;
    [[nodiscard]] auto user_key() const noexcept -> std::string_view;
};
} // namespace stratadb::memtable