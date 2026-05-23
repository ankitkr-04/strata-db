#pragma once

#include "stratadb/config/immutable/skiplist_config.hpp"
#include "stratadb/config/mutable/memtable_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/memtable/memtable_concept.hpp"
#include "stratadb/memtable/memtable_result.hpp"
#include "stratadb/memtable/skiplist_node.hpp"
#include "stratadb/utils/cache.hpp"
#include "stratadb/utils/limits.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace stratadb::memtable {

class SkipListMemTable {
  public:
    // noexcept by design: head allocation failure is unrecoverable and terminates.
    //
    // skiplist_cfg must have been validated by ConfigResolver:
    //   - max_height in [1, utils::MAX_SKIPLIST_HEIGHT]
    //   - branching_factor is a power of two
    explicit SkipListMemTable(memory::Arena& arena,
                              const config::MemTableConfig& memtable_cfg = {},
                              const config::SkipListConfig& skiplist_cfg = {}) noexcept;

    ~SkipListMemTable() noexcept = default;

    SkipListMemTable(const SkipListMemTable&) = delete;
    SkipListMemTable(SkipListMemTable&&) = delete;
    auto operator=(const SkipListMemTable&) -> SkipListMemTable& = delete;
    auto operator=(SkipListMemTable&&) -> SkipListMemTable& = delete;

    [[nodiscard]] auto put(std::string_view key, std::string_view value, memory::TLAB& tlab) noexcept -> PutResult;

    [[nodiscard]] auto remove(std::string_view key, memory::TLAB& tlab) noexcept -> PutResult;

    [[nodiscard("Arena views become invalid after Arena::reset")]]
    auto get(std::string_view key) const noexcept -> std::optional<std::string_view>;

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
            std::forward<Visitor>(visitor)(EntryView{
                .key = node->user_key(),
                .value = node->value(),
                .sequence = node->sequence_number(),
                .type = node->value_type(),
            });
            node = node->next_nodes()[0].load(std::memory_order_acquire);
        }
    }

  private:
    // Compile-time ceiling for the Splice stack frame — must cover the
    // largest possible configured max_height. ConfigResolver enforces
    // max_height <= utils::MAX_SKIPLIST_HEIGHT.
    static constexpr std::size_t MAX_HEIGHT_LIMIT = utils::MAX_SKIPLIST_HEIGHT;

    struct alignas(utils::CACHE_LINE_SIZE) Splice {
        struct Level {
            SkipListNode* prev{nullptr};
            SkipListNode* next{nullptr};
        };
        Level levels[MAX_HEIGHT_LIMIT];
    };

    memory::Arena& arena_;
    std::size_t flush_trigger_bytes_;
    std::size_t stall_trigger_bytes_;
    std::uint8_t max_height_;       // from SkipListConfig, <= MAX_HEIGHT_LIMIT
    std::uint8_t branching_factor_; // from SkipListConfig, power of two
    SkipListNode* head_;

    // Monotonic MVCC sequence; larger = newer. get() probes with UINT64_MAX.
    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::uint64_t> sequence_{0};

    // Writers update both together; keep on one cache line.
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