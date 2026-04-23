#pragma once

#include "stratadb/config/memtable_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/memtable/memtable_concept.hpp"
#include "stratadb/memtable/memtable_result.hpp"
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

    explicit SkipListMemTable(memory::Arena& arena,
                  const config::MemTableConfig& config = config::MemTableConfig{}) noexcept;

    ~SkipListMemTable() noexcept = default;
    SkipListMemTable(const SkipListMemTable&) = delete;
    auto operator=(const SkipListMemTable&) -> SkipListMemTable& = delete;
    SkipListMemTable(SkipListMemTable&&) = delete;
    auto operator=(SkipListMemTable&&) -> SkipListMemTable& = delete;

    [[nodiscard]] auto
    put(std::string_view key, std::string_view value, memory::TLAB& tlab) noexcept -> PutResult;

    [[nodiscard]] auto remove(std::string_view key, memory::TLAB& tlab) noexcept -> PutResult;

    [[nodiscard]] auto get(std::string_view key) const noexcept -> std::optional<std::string_view>;

    [[nodiscard]] auto memory_usage() const noexcept -> std::size_t;

    [[nodiscard]] auto should_flush() const noexcept -> bool;

    struct EntryView {
      std::string_view key;
      std::string_view value;
      std::uint64_t sequence;
      ValueType type;
    };

    template <typename Visitor>
    void scan(Visitor&& visitor) const {
      const SkipListNode* node = head_->next_nodes()[0].load(std::memory_order_acquire);
      while (node != nullptr) {
        visitor(EntryView{
          .key = node->user_key(),
          .value = node->value(),
          .sequence = node->sequence_number(),
          .type = node->value_type(),
        });

        node = node->next_nodes()[0].load(std::memory_order_acquire);
      }
    }

  private:
    struct Splice {
        SkipListNode* prev[SkipListMemTable::MAX_HEIGHT];
        SkipListNode* next[SkipListMemTable::MAX_HEIGHT];
    };

    memory::Arena& arena_;
    const std::size_t flush_trigger_bytes_;
    const std::size_t stall_trigger_bytes_;
    SkipListNode* head_;

    // Monotonic MVCC sequence for versions; larger means newer.
    // A get() probes with UINT64_MAX to retrieve the newest visible version.
    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::uint64_t> sequence_{0};

    // Writer updates sequence_ and memory_usage_ together; keep on one cache line.
    std::atomic<std::size_t> memory_usage_{0};

  private:
    [[nodiscard]] auto make_head() noexcept -> SkipListNode*;

    [[nodiscard]] auto random_height() const noexcept -> std::uint8_t;

    [[nodiscard]] auto find_splice(std::string_view user_key, std::uint64_t seq) const noexcept -> Splice;

    void link_node(SkipListNode* new_node, Splice& splice) noexcept;

    [[nodiscard]] auto insert_node(std::string_view user_key,
                                   std::string_view value,
                                   ValueType type,
                                   std::uint8_t height,
                                   memory::TLAB& tlab) noexcept -> bool;
};

static_assert(IsMemTable<SkipListMemTable>, "SkipListMemTable does not satisfy the MemTable concept");
} // namespace stratadb::memtable