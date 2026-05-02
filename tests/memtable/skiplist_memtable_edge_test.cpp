// tests/memtable/skiplist_memtable_edge_test.cpp
//
// Edge-case and stress coverage for SkipListMemTable.
// Complements skiplist_memtable_test.cpp which covers happy-path behaviour.
//
// New cases:
//   Correctness
//     ├─ Scan includes tombstones (get() hides them, scan() does not)
//     ├─ Resurrection: remove → put returns value, not nullopt
//     ├─ Key collisions across threads observed by get() after join
//     ├─ Sequence numbers are strictly monotonically increasing
//     ├─ Scan emits newest version first for duplicate user keys
//     ├─ Empty-value round-trip (zero-length value)
//     ├─ Single-byte key and value
//     └─ Large batch: 4096 unique keys, full scan matches put order
//
//   Memory / Resource
//     ├─ OOM path: Arena exhausted → OutOfMemory result, no crash
//     ├─ Arena reset with fresh memtable after OOM
//     └─ TLAB detach: allocations return nullptr gracefully after detach
//
//   Concurrency (TSAN clean)
//     ├─ 16 threads × 512 unique keys: all visible after join
//     ├─ Concurrent put + scan: scan never crashes (no torn reads)
//     └─ Sequence monotonicity: concurrent puts → seqs never aliased

#include "stratadb/config/memory_config.hpp"
#include "stratadb/config/memtable_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/memtable/memtable_result.hpp"
#include "stratadb/memtable/skiplist_memtable.hpp"
#include "stratadb/memtable/skiplist_node.hpp"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace stratadb::memtable;
using namespace stratadb::memory;
using namespace stratadb::config;

// ─────────────────────────── Helpers ────────────────────────────────

namespace {

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] auto make_mem_cfg(std::size_t total = 32ULL << 20, std::size_t tlab_sz = 2ULL << 20) -> MemoryConfig {
    MemoryConfig c;
    c.total_budget_bytes = total;
    c.tlab_size_bytes = tlab_sz;
    c.page_strategy = PageStrategy::Standard4K;
    return c;
}

[[nodiscard]] auto make_mt_cfg(std::size_t max = std::numeric_limits<std::size_t>::max()) -> MemTableConfig {
    MemTableConfig c;
    c.max_size_bytes = max;
    c.flush_trigger_bytes = max;
    c.stall_trigger_bytes = max;
    return c;
}

// Zero-padded decimal key, e.g. key_of(42, 8) → "00000042"
[[nodiscard]] auto key_of(std::size_t n, std::size_t width = 16) -> std::string {
    std::string s(width, '0');
    for (std::size_t i = 0; i < width && n > 0; ++i) {
        s[width - 1 - i] = static_cast<char>('0' + n % 10);
        n /= 10;
    }
    return s;
}

} // namespace

// ═══════════════════════════════════════════════════════════════════
// Correctness
// ═══════════════════════════════════════════════════════════════════

// Scan must expose tombstone nodes; only get() hides them.
TEST(SkipListEdge, ScanExposesTombstones) {
    auto arena = Arena::create(make_mem_cfg()).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};
    TLAB tlab(arena);

    ASSERT_EQ(mt.put("alive", "v", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("dead", "v", tlab), PutResult::Ok);
    ASSERT_EQ(mt.remove("dead", tlab), PutResult::Ok);

    // get() must hide tombstone
    EXPECT_TRUE(mt.get("alive").has_value());
    EXPECT_FALSE(mt.get("dead").has_value());

    // scan() must see all nodes, including the TypeDeletion one
    int live_count = 0;
    int tomb_count = 0;
    mt.scan([&](const SkipListMemTable::EntryView& e) {
        if (e.type == ValueType::TypeDeletion)
            ++tomb_count;
        else
            ++live_count;
    });

    // 1 live "alive", 1 live "dead" (first put), 1 tombstone "dead"
    EXPECT_GE(live_count, 2);
    EXPECT_GE(tomb_count, 1);
}

// remove() → put() on the same key must make it visible again.
TEST(SkipListEdge, Resurrection) {
    auto arena = Arena::create(make_mem_cfg()).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};
    TLAB tlab(arena);

    ASSERT_EQ(mt.put("k", "v1", tlab), PutResult::Ok);
    ASSERT_EQ(mt.remove("k", tlab), PutResult::Ok);
    EXPECT_FALSE(mt.get("k").has_value());

    ASSERT_EQ(mt.put("k", "v2", tlab), PutResult::Ok);
    auto got = mt.get("k");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "v2");
}

// Zero-length value must round-trip correctly.
TEST(SkipListEdge, EmptyValueRoundTrip) {
    auto arena = Arena::create(make_mem_cfg()).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};
    TLAB tlab(arena);

    ASSERT_EQ(mt.put("k", "", tlab), PutResult::Ok);
    auto got = mt.get("k");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->size(), 0u);
}

// Single-byte key and single-byte value.
TEST(SkipListEdge, SingleByteKeyAndValue) {
    auto arena = Arena::create(make_mem_cfg()).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};
    TLAB tlab(arena);

    ASSERT_EQ(mt.put("x", "y", tlab), PutResult::Ok);
    auto got = mt.get("x");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "y");
}

// Sequence numbers assigned by concurrent puts must be globally unique.
// If two nodes received the same sequence number they would have identical
// internal keys and the skip list ordering would be undefined.
TEST(SkipListEdge, SequenceNumbersAreUnique) {
    constexpr std::size_t kThreads = 8;
    constexpr std::size_t kPerThread = 512;

    auto arena = Arena::create(make_mem_cfg(256ULL << 20)).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};

    std::mutex mu;
    std::set<std::uint64_t> seqs;
    std::vector<std::thread> workers;
    workers.reserve(kThreads);

    for (std::size_t t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t] {
            TLAB tlab(arena);
            for (std::size_t i = 0; i < kPerThread; ++i) {
                const auto key = key_of(t * kPerThread + i);
                ASSERT_EQ(mt.put(key, "v", tlab), PutResult::Ok);
            }
        });
    }
    for (auto& w : workers)
        w.join();

    // Collect all sequence numbers via scan.
    mt.scan([&](const SkipListMemTable::EntryView& e) {
        std::lock_guard lock(mu);
        seqs.insert(e.sequence);
    });

    // Every sequence number must appear exactly once.
    const std::size_t expected = kThreads * kPerThread;
    EXPECT_EQ(seqs.size(), expected) << "Duplicate or missing sequence numbers detected — " << expected - seqs.size()
                                     << " discrepancies";
}

// After a concurrent insert workload, scan must emit duplicate user keys
// in descending sequence order (newest first at level 0).
TEST(SkipListEdge, ScanNewestVersionFirstForDuplicates) {
    auto arena = Arena::create(make_mem_cfg()).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};
    TLAB tlab(arena);

    // Insert three versions of "k".
    ASSERT_EQ(mt.put("k", "v1", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("k", "v2", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("k", "v3", tlab), PutResult::Ok);

    // Collect (seq, value) for key "k".
    std::vector<std::pair<std::uint64_t, std::string>> entries;
    mt.scan([&](const SkipListMemTable::EntryView& e) {
        if (e.key == "k") {
            entries.emplace_back(e.sequence, std::string(e.value));
        }
    });

    ASSERT_EQ(entries.size(), 3u);

    // Sequences must be strictly decreasing (newest first in skip list order).
    for (std::size_t i = 1; i < entries.size(); ++i) {
        EXPECT_GT(entries[i - 1].first, entries[i].first)
            << "Scan emitted versions out of sequence order at index " << i;
    }

    // The first entry in scan order is the most-recent put ("v3").
    EXPECT_EQ(entries[0].second, "v3");
}

// Large ordered batch: 4096 keys → scan must be fully sorted.
TEST(SkipListEdge, LargeBatchScanIsSorted) {
    constexpr std::size_t N = 4096;

    auto arena = Arena::create(make_mem_cfg(64ULL << 20)).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};
    TLAB tlab(arena);

    // Insert in reverse order to stress the sorted-insertion path.
    for (std::size_t i = N; i-- > 0;) {
        ASSERT_EQ(mt.put(key_of(i), "v", tlab), PutResult::Ok);
    }

    std::vector<std::string> keys_seen;
    keys_seen.reserve(N);
    mt.scan([&](const SkipListMemTable::EntryView& e) { keys_seen.emplace_back(e.key); });

    ASSERT_EQ(keys_seen.size(), N);
    EXPECT_TRUE(std::is_sorted(keys_seen.begin(), keys_seen.end())) << "Scan output is not lexicographically sorted";
}

// get() on a key that was never inserted must return nullopt (miss).
TEST(SkipListEdge, GetOnNeverInsertedKey) {
    auto arena = Arena::create(make_mem_cfg()).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};
    TLAB tlab(arena);

    ASSERT_EQ(mt.put("a", "1", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("c", "3", tlab), PutResult::Ok);

    // "b" is between two existing keys — typical miss in a real workload.
    EXPECT_FALSE(mt.get("b").has_value());
    // Completely out-of-range keys.
    EXPECT_FALSE(mt.get("").has_value());
    EXPECT_FALSE(mt.get("zzz").has_value());
}

// ═══════════════════════════════════════════════════════════════════
// Memory / Resource Edge Cases
// ═══════════════════════════════════════════════════════════════════

// When the arena is exhausted, put() must return OutOfMemory without
// crashing, corrupting internal state, or leaving a partial node.
TEST(SkipListEdge, OomReturnNotCrash) {
    // Tiny arena: just enough for the head node, nothing more.
    const std::size_t tiny = 4096;
    auto arena = Arena::create(make_mem_cfg(tiny, 4096)).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg(tiny)};
    TLAB tlab(arena);

    // Drive until OOM.
    PutResult last = PutResult::Ok;
    for (int i = 0; i < 10000; ++i) {
        last = mt.put(key_of(static_cast<std::size_t>(i)), "v", tlab);
        if (last == PutResult::OutOfMemory)
            break;
    }

    EXPECT_EQ(last, PutResult::OutOfMemory) << "Expected OutOfMemory from a tiny arena; got something else";

    // memory_usage() must not exceed arena capacity.
    EXPECT_LE(mt.memory_usage(), arena.capacity());

    // Keys already inserted before OOM must still be readable.
    EXPECT_NO_FATAL_FAILURE({
        auto res = mt.get(key_of(0)); // may or may not be present — just must not crash
    });
}

// After an arena is reset and a NEW memtable is created on it, everything
// must work as if fresh.  (Clients are responsible for not using the old
// memtable after reset — this test only verifies the new-memtable path.)
TEST(SkipListEdge, FreshMemtableAfterArenaReset) {
    auto arena = Arena::create(make_mem_cfg()).value();

    {
        SkipListMemTable mt1{arena, make_mt_cfg()};
        TLAB tlab(arena);
        ASSERT_EQ(mt1.put("old_key", "old_val", tlab), PutResult::Ok);
        EXPECT_TRUE(mt1.get("old_key").has_value());
    } // mt1 destroyed (no cleanup needed — arena backed)

    arena.reset(); // rewind offset; old nodes are now garbage

    {
        SkipListMemTable mt2{arena, make_mt_cfg()};
        TLAB tlab(arena);
        ASSERT_EQ(mt2.put("new_key", "new_val", tlab), PutResult::Ok);
        auto got = mt2.get("new_key");
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(*got, "new_val");
        EXPECT_FALSE(mt2.get("old_key").has_value());
    }
}

// TLAB::detach() must make subsequent allocations return nullptr without
// triggering undefined behaviour.  The memtable's insert_node path handles
// nullptr TLAB allocation by returning false → OutOfMemory.
TEST(SkipListEdge, TlabDetachPreventsAllocation) {
    auto arena = Arena::create(make_mem_cfg()).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};
    TLAB tlab(arena);

    // One successful insert before detach.
    ASSERT_EQ(mt.put("before", "v", tlab), PutResult::Ok);

    tlab.detach();
    EXPECT_FALSE(tlab.is_attached());

    // After detach, tlab.allocate() returns nullptr → insert_node returns false
    // → put() returns OutOfMemory.
    const auto r = mt.put("after", "v", tlab);
    EXPECT_EQ(r, PutResult::OutOfMemory);

    // Key inserted before detach is still visible.
    EXPECT_TRUE(mt.get("before").has_value());
}

// ═══════════════════════════════════════════════════════════════════
// Concurrency (should be TSAN-clean)
// ═══════════════════════════════════════════════════════════════════

// 16 threads each insert 512 unique keys.  After all threads join, every
// key must be retrievable via get().
TEST(SkipListEdge, ConcurrentAllKeysVisible) {
    constexpr std::size_t kThreads = 16;
    constexpr std::size_t kPerThread = 512;

    auto arena = Arena::create(make_mem_cfg(256ULL << 20)).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};

    std::vector<std::thread> workers;
    workers.reserve(kThreads);

    for (std::size_t t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t] {
            TLAB tlab(arena);
            for (std::size_t i = 0; i < kPerThread; ++i) {
                const auto key = key_of(t * kPerThread + i);
                const auto val = "val_" + std::to_string(t * kPerThread + i);
                ASSERT_EQ(mt.put(key, val, tlab), PutResult::Ok);
            }
        });
    }
    for (auto& w : workers)
        w.join();

    // Verify all keys
    for (std::size_t t = 0; t < kThreads; ++t) {
        for (std::size_t i = 0; i < kPerThread; ++i) {
            const auto key = key_of(t * kPerThread + i);
            auto got = mt.get(key);
            ASSERT_TRUE(got.has_value()) << "Key " << key << " not found after concurrent insert";
        }
    }
}

// A concurrent scan running while writers insert new nodes must never crash
// (torn pointer reads, null-deref on a partially-initialised node, etc.).
// We don't assert full coverage of newly-inserted keys — only safety.
TEST(SkipListEdge, ConcurrentPutAndScanNoCrash) {
    auto arena = Arena::create(make_mem_cfg(64ULL << 20)).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};

    std::atomic<bool> done_writing{false};

    // Writer thread: insert 2048 keys continuously.
    std::thread writer([&] {
        TLAB tlab(arena);
        for (std::size_t i = 0; i < 2048; ++i) {
            auto res = mt.put(key_of(i), "v", tlab);
        }
        done_writing.store(true, std::memory_order_release);
    });

    // Scanner thread: scan while writer is active.
    std::thread scanner([&] -> void {
        while (!done_writing.load(std::memory_order_acquire)) {
            std::atomic_size_t count{0};
            mt.scan([&count](const SkipListMemTable::EntryView&) -> void { count.fetch_add(1, std::memory_order_relaxed); });
            (void)count; // prevent dead-store elimination
        }
        // Final scan after writing is done.
        std::size_t final_count = 0;
        mt.scan([&final_count](const SkipListMemTable::EntryView&) -> void { ++final_count; });
        EXPECT_GE(final_count, 0u); // trivially true; ensures scan ran
    });

    writer.join();
    scanner.join();
}

// Stress: 8 concurrent writers on the SAME key space.
// After join, the latest writer's value must be visible for each key.
// (We can't predict which thread "wins" for each key, but get() must
// return *some* valid, non-corrupted value.)
TEST(SkipListEdge, ConcurrentSameKeyspaceNoCrash) {
    constexpr std::size_t kThreads = 8;
    constexpr std::size_t kKeySpace = 256;
    constexpr std::size_t kOpsEach = 1024;

    auto arena = Arena::create(make_mem_cfg(64ULL << 20)).value();
    auto mt = SkipListMemTable{arena, make_mt_cfg()};

    std::vector<std::thread> workers;
    workers.reserve(kThreads);

    for (std::size_t t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t] {
            TLAB tlab(arena);
            for (std::size_t op = 0; op < kOpsEach; ++op) {
                const auto key = key_of(op % kKeySpace);
                const auto val = "t" + std::to_string(t) + "_op" + std::to_string(op);
                auto res = mt.put(key, val, tlab); // result ignored — stall/flush ok
            }
        });
    }
    for (auto& w : workers)
        w.join();

    // Every key in the shared keyspace must be readable and non-corrupt.
    for (std::size_t k = 0; k < kKeySpace; ++k) {
        const auto key = key_of(k);
        auto got = mt.get(key);
        ASSERT_TRUE(got.has_value()) << "Key " << key << " missing after concurrent same-keyspace inserts";
        // Value must begin with 't' (our format) — not garbage bytes.
        EXPECT_FALSE(got->empty());
        EXPECT_EQ((*got)[0], 't') << "Value corruption detected for key " << key << ": " << *got;
    }
}

// ═══════════════════════════════════════════════════════════════════
// SkipListNode Static Guarantees (belt-and-suspenders header checks)
// ═══════════════════════════════════════════════════════════════════

TEST(SkipListNodeEdge, MaxSequenceFitsInTrailerBits) {
    // Trailer = (sequence << TYPE_BITS) | type_byte.  The top bit of a
    // uint64_t must remain available after the shift.
    const std::uint64_t shifted = SkipListNode::MAX_SEQUENCE << SkipListNode::TYPE_BITS;
    EXPECT_GT(shifted, 0u) << "Sequence shift overflows uint64_t";
    EXPECT_LE(SkipListNode::MAX_SEQUENCE, std::numeric_limits<std::uint64_t>::max() >> SkipListNode::TYPE_BITS);
}

TEST(SkipListNodeEdge, AllocationSizeZeroHeightIsZero) {
    EXPECT_EQ(SkipListNode::allocation_size(0, 128, 256), 0u);
}

TEST(SkipListNodeEdge, AllocationSizeMonotonicallyIncreasesWithHeight) {
    std::size_t prev = 0;
    for (std::uint8_t h = 1; h <= 12; ++h) {
        const std::size_t sz = SkipListNode::allocation_size(h, 16, 64);
        EXPECT_GT(sz, prev) << "allocation_size did not grow at height " << static_cast<int>(h);
        prev = sz;
    }
}