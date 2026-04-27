#include "stratadb/memtable/skiplist_memtable.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <limits>
#include <new>

namespace stratadb::memtable {
namespace {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
thread_local std::uint64_t tl_rng = [] -> unsigned long {
    auto seed = reinterpret_cast<uintptr_t>(&tl_rng);

    seed ^= seed >> 33;
    seed *= UINT64_C(0xff51afd7ed558ccd);
    seed ^= seed >> 33;
    seed *= UINT64_C(0xc4ceb9fe1a85ec53);
    seed ^= seed >> 33;
    return seed != 0 ? seed : UINT64_C(0xdeadbeefcafe1234);
}();

auto xorshift64() noexcept -> std::uint64_t {
    tl_rng ^= tl_rng >> 13;
    tl_rng ^= tl_rng << 7;
    tl_rng ^= tl_rng >> 17;
    return tl_rng;
};

[[nodiscard]] auto compare_impl(const SkipListNode* node, std::string_view user_key, std::uint64_t seq) noexcept
    -> int {

    const std::string_view node_uk = node->user_key();
    const std::size_t prefix_len = std::min(std::min(node_uk.size(), user_key.size()), SkipListNode::PREFIX_BYTES);

    int cmp = std::memcmp(node->prefix_.data(), user_key.data(), prefix_len);

    if (cmp == 0) {
        const bool node_beyond = node_uk.size() > prefix_len;
        const bool srch_beyond = user_key.size() > prefix_len;

        if (node_beyond || srch_beyond) {
            const std::size_t min_len = std::min(node_uk.size(), user_key.size());
            cmp = std::memcmp(node_uk.data() + prefix_len, user_key.data() + prefix_len, min_len - prefix_len);
        }

        if (cmp == 0) {
            if (node_uk.size() < user_key.size()) {
                cmp = -1;
            } else if (node_uk.size() > user_key.size()) {
                cmp = +1;
            }
        }
    }

    if (cmp != 0)
        return cmp;

    const std::uint64_t node_seq = node->sequence_number();
    if (node_seq > seq) {
        return -1;
    } else if (node_seq < seq) {
        return +1;
    }
    return 0;
};

[[nodiscard]] auto walk_forward(SkipListNode* cur, int level, std::string_view user_key, std::uint64_t seq) noexcept
    -> std::pair<SkipListNode*, SkipListNode*> {
    const auto level_index = static_cast<std::size_t>(level);

    while (true) {
        SkipListNode* next = cur->next_nodes()[level_index].load(std::memory_order_acquire);
        if (!next) {
            return {cur, nullptr};
        }

        const int cmp = compare_impl(next, user_key, seq);
        if (cmp >= 0) {

            return {cur, next};
        }
        cur = next;
    }
};

} // namespace

SkipListMemTable::SkipListMemTable(memory::Arena& arena, const config::MemTableConfig& config) noexcept
    : arena_(arena)
    , flush_trigger_bytes_(config.flush_trigger_bytes)
    , stall_trigger_bytes_(config.stall_trigger_bytes)
    , head_(make_head()) {
    if (!head_) {
        // Head allocation failure means the Arena itself is broken.
        // There is no meaningful recovery path; terminate early.
        std::terminate();
    }
}

auto SkipListMemTable::make_head() noexcept -> SkipListNode* {
    const std::size_t sz = SkipListNode::allocation_size(MAX_HEIGHT, 0, 0);

    void* mem = arena_.allocate_aligned(sz, SkipListNode::REQUIRED_ALIGNMENT);
    if (!mem) {
        return nullptr;
    }

    auto* head = static_cast<SkipListNode*>(mem);
    head->key_len_ = SkipListNode::TRAILER_BYTES;
    head->val_len_ = 0;
    head->height_ = MAX_HEIGHT;
    std::memset(head->prefix_.data(), 0, SkipListNode::PREFIX_BYTES);

    auto tower = head->next_nodes();
    for (std::uint8_t i = 0; i < MAX_HEIGHT; ++i) {
        new (&tower[i]) std::atomic<SkipListNode*>{nullptr};
    }

    return head;
};

auto SkipListMemTable::random_height() const noexcept -> std::uint8_t {
    std::uint8_t height = 1;
    static_assert(std::has_single_bit(static_cast<unsigned int>(BRANCHING_FACTOR)),
                  "BRANCHING_FACTOR must be a power of two for unbiased height distribution");
    while (height < MAX_HEIGHT && (xorshift64() & (BRANCHING_FACTOR - 1u)) == 0) {
        ++height;
    }
    return height;
};

[[nodiscard]] auto SkipListMemTable::find_splice(std::string_view user_key, std::uint64_t seq) const noexcept
    -> Splice {
    Splice splice{};
    SkipListNode* cur = head_;

    for (int level = MAX_HEIGHT - 1; level >= 0; --level) {
        auto [prev, next] = walk_forward(cur, level, user_key, seq);
        splice.Levels[level].prev = prev;
        splice.Levels[level].next = next;
        cur = prev;
    }

    return splice;
};

void SkipListMemTable::link_node(SkipListNode* new_node, Splice& splice) noexcept {
    const std::string_view uk = new_node->user_key();
    const std::uint64_t seq = new_node->sequence_number();
    const std::uint8_t height = new_node->height_;

    for (std::uint8_t level = 0; level < height; ++level) {
        while (true) {
            SkipListNode* expected_next = splice.Levels[level].next;
            new_node->next_nodes()[level].store(expected_next, std::memory_order_relaxed);

            const bool cas_ok =
                splice.Levels[level].prev->next_nodes()[level].compare_exchange_strong(expected_next,
                                                                                       new_node,
                                                                                       std::memory_order_release,
                                                                                       std::memory_order_acquire);

            if (cas_ok) [[likely]] {
                break;
            }

            // A failed CAS means concurrent writers changed the search frontier.
            // we assume from the failed predecessar at exact same level, so we need to walk forward again to find the
            // correct position for new_node at this level.
            SkipListNode* cur = splice.Levels[level].prev;
            auto [new_prev, new_next] = walk_forward(cur, level, uk, seq);
            splice.Levels[level].prev = new_prev;
            splice.Levels[level].next = new_next;
        }
    }
};

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] auto SkipListMemTable::insert_node(std::string_view user_key,
                                                 std::string_view value,
                                                 ValueType type,
                                                 std::uint8_t height,
                                                 memory::TLAB& tlab) noexcept -> bool {

    const std::size_t size = SkipListNode::allocation_size(height, user_key.size(), value.size());

    if (size == 0) {
        // Integer overflow in size calculation; treat as OOM.
        return false;
    }

    void* mem = tlab.allocate(size, SkipListNode::REQUIRED_ALIGNMENT);

    if (!mem) {
        return false;
    }

    const std::uint64_t seq = sequence_.fetch_add(1, std::memory_order_relaxed);
    if (seq > SkipListNode::MAX_SEQUENCE) [[unlikely]] {
        std::fputs("SkipListMemTable sequence exceeded 56-bit trailer range\n", stderr);
        std::terminate();
    }

    SkipListNode* new_node = &SkipListNode::construct(mem, user_key, value, seq, type, height);

    Splice splice = find_splice(user_key, seq);
    link_node(new_node, splice);

    memory_usage_.fetch_add(size, std::memory_order_relaxed);
    return true;
};

[[nodiscard]] auto SkipListMemTable::put(std::string_view key, std::string_view value, memory::TLAB& tlab) noexcept
    -> PutResult {
    const std::size_t used = memory_usage();
    if (used >= stall_trigger_bytes_) {
        return PutResult::StallNeeded;
    }
    if (used >= flush_trigger_bytes_) {
        return PutResult::FlushNeeded;
    }
    return insert_node(key, value, ValueType::TypeValue, random_height(), tlab) ? PutResult::Ok
                                                                                : PutResult::OutOfMemory;
}

[[nodiscard]] auto SkipListMemTable::remove(std::string_view key, memory::TLAB& tlab) noexcept -> PutResult {
    const std::size_t used = memory_usage();
    if (used >= stall_trigger_bytes_) {
        return PutResult::StallNeeded;
    }
    if (used >= flush_trigger_bytes_) {
        return PutResult::FlushNeeded;
    }

    return insert_node(key, {}, ValueType::TypeDeletion, 1, tlab) ? PutResult::Ok : PutResult::OutOfMemory;
}
[[nodiscard]] auto SkipListMemTable::get(std::string_view key) const noexcept -> std::optional<std::string_view> {
    const std::uint64_t k_max_seq = std::numeric_limits<std::uint64_t>::max();

    const SkipListNode* cur = head_;

    for (int level = MAX_HEIGHT - 1; level >= 0; --level) {
        const std::size_t level_index = static_cast<std::size_t>(level);

        while (true) {
            const SkipListNode* next = cur->next_nodes()[level_index].load(std::memory_order_acquire);
            if (!next) {
                break;
            }

            const int cmp = compare_impl(next, key, k_max_seq);
            if (cmp >= 0) {
                break;
            }

            cur = next;
        }
    }

    const SkipListNode* candidate = cur->next_nodes()[0].load(std::memory_order_acquire);
    if (candidate == nullptr) {
        return std::nullopt;
    }

    if (candidate->user_key() != key) {
        return std::nullopt;
    }

    if (candidate->is_tombstone()) {
        return std::nullopt;
    }

    return candidate->value();
};

[[nodiscard]] auto SkipListMemTable::memory_usage() const noexcept -> std::size_t {
    return memory_usage_.load(std::memory_order_relaxed);
}

[[nodiscard]] auto SkipListMemTable::should_flush() const noexcept -> bool {
    return memory_usage() >= flush_trigger_bytes_;
}
} // namespace stratadb::memtable