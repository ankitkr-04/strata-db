#include "stratadb/memtable/skiplist_memtable.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <limits>
#include <new>

namespace stratadb::memtable {
namespace {
struct Splice {
    SkipListNode* prev[SkipListMemTable::MAX_HEIGHT];
    SkipListNode* next[SkipListMemTable::MAX_HEIGHT];
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
thread_local std::uint64_t tl_rng = [] {
    auto seed = reinterpret_cast<uintptr_t>(&tl_rng);

    seed ^= seed >> 33;
    seed *= UINT64_C(0xff51afd7ed558ccd);
    seed ^= seed >> 33;
    seed *= UINT64_C(0xc4ceb9fe1a85ec53);
    seed ^= seed >> 33;
    return seed != 0 ? seed : UINT64_C(0xdeadbeefcafe1234);
}();

inline auto xorshift64() noexcept -> std::uint64_t {
    tl_rng ^= tl_rng >> 13;
    tl_rng ^= tl_rng << 7;
    tl_rng ^= tl_rng >> 17;
    return tl_rng;
};

[[nodiscard]] auto compare_impl(const SkipListNode* node, std::string_view user_key, std::uint64_t seq) noexcept
    -> int {

    const std::string_view node_uk = node->user_key();
    const std::size_t prefix_len = std::min({node_uk.size(), user_key.size(), std::size_t{7}});

    int cmp = std::memcmp(node->prefix_, user_key.data(), prefix_len);

    if (cmp == 0) {
        const bool node_beyond = node_uk.size() > prefix_len;
        const bool srch_beyond = user_key.size() > prefix_len;

        if (node_beyond || srch_beyond) {
            const std::size_t min_len = std::min(node_uk.size(), user_key.size());
            cmp = std::memcmp(node_uk.data(), user_key.data(), min_len);
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
    if (node_seq < seq) {
        return -1;
    } else if (node_seq > seq) {
        return +1;
    }
    return 0;
};

} // namespace

auto SkipListMemTable::compare(const SkipListNode* node, std::string_view user_key, std::uint64_t seq) const noexcept
    -> int {
    return compare_impl(node, user_key, seq);
}

SkipListMemTable::SkipListMemTable(memory::Arena& arena, memory::EpochManager& epoch_manager) noexcept
    : arena_(arena)
    , epoch_manager_(epoch_manager)
    , head_(make_head()) {
    if (!head_) {
        // Head allocation failure means the Arena itself is broken.
        // There is no meaningful recovery path; terminate early.
        std::terminate();
    }
};

auto SkipListMemTable::make_head() noexcept -> SkipListNode* {
    const std::size_t sz = SkipListNode::allocation_size(MAX_HEIGHT, 0, 0);

    void* mem = arena_.allocate_aligned(sz, 8);
    if (!mem) {
        return nullptr;
    }

    auto* head = static_cast<SkipListNode*>(mem);
    head->key_len_ = 8;
    head->val_len_ = 0;
    head->height_ = MAX_HEIGHT;
    std::memset(head->prefix_, 0, 7);

    auto* tower = head->next_nodes();
    for (std::uint8_t i = 0; i < MAX_HEIGHT; ++i) {
        new (tower + i) std::atomic<SkipListNode*>{nullptr};
    }

    return head;
};

} // namespace stratadb::memtable