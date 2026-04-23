#pragma once

#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/epoch_manager.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/memtable/memtable_concept.hpp"
#include "stratadb/memtable/skiplist_node.hpp"
#include "stratadb/utils/hardware.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace stratadb::memtable {
class SkipListMemTable {
  public:
    static constexpr std::uint8_t MAX_HEIGHT = 12;
    static constexpr std::uint8_t BRANCHING_FACTOR = 4; // 1 in 4 chance to increase height at each level

    explicit SkipListMemTable(memory::Arena& arena, memory::EpochManager& epoch_manager) noexcept;

    ~SkipListMemTable() noexcept;

    SkipListMemTable(const SkipListMemTable&) = delete;
    auto operator=(const SkipListMemTable&) -> SkipListMemTable& = delete;
    SkipListMemTable(SkipListMemTable&&) = delete;
    auto operator=(SkipListMemTable&&) -> SkipListMemTable& = delete;

    [[nodiscard]] auto
    put(std::string_view key, std::string_view value, memory::TLAB& tlab, std::size_t flush_trigger_bytes) noexcept
        -> bool;

    [[nodiscard]] auto remove(std::string_view key, memory::TLAB& tlab) noexcept -> bool;

    [[nodiscard]] auto get(std::string_view key) const noexcept -> std::optional<std::string_view>;

    [[nodiscard]] auto memory_usage() const noexcept -> std::size_t;

    [[nodiscard]] auto should_flush(std::size_t flush_trigger_bytes) const noexcept -> bool;

  private:
    [[nodiscard]]auto make_head() noexcept -> SkipListNode*;

    [[nodiscard]] auto random_height() const noexcept -> std::uint8_t;

    [[nodiscard]] auto compare(const SkipListNode* node, std::string_view user_key, std::uint64_t seq) const noexcept
        -> int;

    [[nodiscard]] auto
    find_splice(std::string_view user_key, std::uint64_t seq, SkipListNode* prev, SkipListNode* next) const noexcept;

    void link_node(SkipListNode* new_node, SkipListNode* prev, SkipListNode* next) noexcept;

    [[nodiscard]] auto insert_node(std::string_view user_key,
                                   std::string_view value,
                                   ValueType type,
                                   std::uint8_t height,
                                   memory::TLAB& tlab) noexcept -> bool;

  private:
    memory::Arena& arena_;
    memory::EpochManager& epoch_manager_;
    SkipListNode* head_;

    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::uint64_t> sequence_{0};

    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::size_t> memory_usage_{0};
};

static_assert(IsMemTable<SkipListMemTable>, "SkipListMemTable does not satisfy the MemTable concept");
} // namespace stratadb::memtable