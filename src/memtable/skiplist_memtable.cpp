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



} // namespace
} // namespace stratadb::memtable