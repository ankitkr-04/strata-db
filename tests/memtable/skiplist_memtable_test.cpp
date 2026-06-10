#include "../support/common.hpp"
#include "../support/memtable_fixture.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/memtable/skiplist_memtable.hpp"
#include "stratadb/memtable/skiplist_node.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

using stratadb::config::MemTableConfig;
using stratadb::memory::TLAB;
using stratadb::memtable::IsMemTable;
using stratadb::memtable::PutResult;
using stratadb::memtable::SkipListMemTable;
using stratadb::memtable::SkipListNode;
using stratadb::memtable::ValueType;
using stratadb::test::do_not_optimize;
using stratadb::test::make_test_arena;
using stratadb::test::make_test_memtable;

namespace {

using SkipListMemTableTest = stratadb::test::MemTableFixture;

[[nodiscard]] auto big_string(char ch, std::size_t n) -> std::string {
    return std::string(n, ch);
}

[[nodiscard]] auto key_of(std::size_t n, std::size_t width = 16) -> std::string {
    std::string s(width, '0');
    for (std::size_t i = 0; i < width && n > 0; ++i) {
        s[width - 1 - i] = static_cast<char>('0' + n % 10);
        n /= 10;
    }
    return s;
}

} // namespace

TEST(SkipListMemTable, ConceptSatisfied) {
    static_assert(IsMemTable<SkipListMemTable>);
    SUCCEED();
}

TEST_F(SkipListMemTableTest, EmptyGetReturnsNullopt) {
    EXPECT_FALSE(mt.get("missing").has_value());
    EXPECT_EQ(mt.memory_usage(), 0u);
}

TEST_F(SkipListMemTableTest, PutGetAndOverwriteLatestWins) {
    ASSERT_EQ(mt.put("k1", "v1", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("k1", "v2", tlab), PutResult::Ok);

    auto got = mt.get("k1");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "v2");
    EXPECT_GT(mt.memory_usage(), 0u);
}

TEST_F(SkipListMemTableTest, RemoveCreatesTombstone) {
    ASSERT_EQ(mt.put("dead", "alive", tlab), PutResult::Ok);
    ASSERT_EQ(mt.remove("dead", tlab), PutResult::Ok);

    EXPECT_FALSE(mt.get("dead").has_value());
}

TEST(SkipListMemTable, FlushThresholdBlocksWrites) {
    auto arena = make_test_arena();

    MemTableConfig cfg{};
    cfg.flush_trigger_bytes = 0;
    cfg.stall_trigger_bytes = std::numeric_limits<std::size_t>::max();

    auto memtable = make_test_memtable(arena, cfg);
    TLAB tlab(arena);

    EXPECT_TRUE(memtable.should_flush());
    EXPECT_EQ(memtable.put("blocked", "v", tlab), PutResult::FlushNeeded);
    EXPECT_EQ(memtable.remove("blocked", tlab), PutResult::FlushNeeded);
}

TEST(SkipListMemTable, StallThresholdReturnsStallNeeded) {
    auto arena = make_test_arena();

    MemTableConfig cfg{};
    cfg.flush_trigger_bytes = std::numeric_limits<std::size_t>::max();
    cfg.stall_trigger_bytes = 0;

    auto memtable = make_test_memtable(arena, cfg);
    TLAB tlab(arena);

    EXPECT_FALSE(memtable.should_flush());
    EXPECT_EQ(memtable.put("blocked", "v", tlab), PutResult::StallNeeded);
    EXPECT_EQ(memtable.remove("blocked", tlab), PutResult::StallNeeded);
}

TEST_F(SkipListMemTableTest, MemoryUsageTracksSuccessfulInserts) {
    const auto before = mt.memory_usage();
    ASSERT_EQ(mt.put("a", "1", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("b", "2", tlab), PutResult::Ok);
    const auto after = mt.memory_usage();

    EXPECT_GT(after, before);
}

TEST_F(SkipListMemTableTest, LongKeyRoundTrip) {
    const std::string key = big_string('k', 64);
    const std::string value = big_string('v', 128);

    ASSERT_EQ(mt.put(key, value, tlab), PutResult::Ok);

    auto got = mt.get(key);
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, value);
}

TEST_F(SkipListMemTableTest, LargeBatchRoundTrip) {
    constexpr std::size_t kEntries = 512;

    for (std::size_t i = 0; i < kEntries; ++i) {
        const auto key = "key_" + std::to_string(i);
        const auto value = "val_" + std::to_string(i);
        ASSERT_EQ(mt.put(key, value, tlab), PutResult::Ok);
    }

    for (std::size_t i = 0; i < kEntries; ++i) {
        const auto key = "key_" + std::to_string(i);
        const auto value = "val_" + std::to_string(i);
        auto got = mt.get(key);
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(*got, value);
    }
}

TEST_F(SkipListMemTableTest, ScanProvidesSortedForwardView) {
    ASSERT_EQ(mt.put("b", "1", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("a", "1", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("b", "2", tlab), PutResult::Ok);

    std::vector<std::string> keys;
    std::vector<std::uint64_t> seqs;

    mt.scan([&](const SkipListMemTable::EntryView& entry) {
        keys.emplace_back(entry.key);
        seqs.push_back(entry.sequence);
    });

    ASSERT_EQ(keys.size(), 3u);
    EXPECT_TRUE(std::is_sorted(keys.begin(), keys.end()));

    ASSERT_EQ(keys[1], "b");
    ASSERT_EQ(keys[2], "b");
    EXPECT_GT(seqs[1], seqs[2]);
}

TEST_F(SkipListMemTableTest, ConcurrentUniqueInserts) {
    constexpr std::size_t kThreads = 4;
    constexpr std::size_t kPerThread = 128;

    std::array<std::thread, kThreads> workers{};
    for (std::size_t t = 0; t < kThreads; ++t) {
        workers[t] = std::thread([&, t] {
            TLAB local_tlab(arena);
            for (std::size_t i = 0; i < kPerThread; ++i) {
                const auto key = "t" + std::to_string(t) + "_k" + std::to_string(i);
                const auto value = "v" + std::to_string(i);
                ASSERT_EQ(mt.put(key, value, local_tlab), PutResult::Ok);
            }
        });
    }

    for (auto& th : workers) {
        th.join();
    }

    for (std::size_t t = 0; t < kThreads; ++t) {
        for (std::size_t i = 0; i < kPerThread; ++i) {
            const auto key = "t" + std::to_string(t) + "_k" + std::to_string(i);
            const auto value = "v" + std::to_string(i);
            auto got = mt.get(key);
            ASSERT_TRUE(got.has_value());
            EXPECT_EQ(*got, value);
        }
    }
}

TEST_F(SkipListMemTableTest, ConcurrentSameKeyLastWriterVisible) {
    constexpr std::size_t kThreads = 8;
    std::array<std::thread, kThreads> workers{};

    for (std::size_t t = 0; t < kThreads; ++t) {
        workers[t] = std::thread([&, t] {
            TLAB local_tlab(arena);
            const auto value = "v" + std::to_string(t);
            ASSERT_EQ(mt.put("shared", value, local_tlab), PutResult::Ok);
        });
    }

    for (auto& th : workers) {
        th.join();
    }

    auto got = mt.get("shared");
    ASSERT_TRUE(got.has_value());
    EXPECT_FALSE(got->empty());
}

TEST(SkipListMemTable, RejectsOverflowSizedPayloads) {
    EXPECT_EQ(SkipListNode::allocation_size(1, std::numeric_limits<std::size_t>::max(), 1), 0u);
    EXPECT_EQ(SkipListNode::allocation_size(1, 1, std::numeric_limits<std::size_t>::max()), 0u);
}

TEST_F(SkipListMemTableTest, ScanExposesTombstones) {
    ASSERT_EQ(mt.put("alive", "v", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("dead", "v", tlab), PutResult::Ok);
    ASSERT_EQ(mt.remove("dead", tlab), PutResult::Ok);

    EXPECT_TRUE(mt.get("alive").has_value());
    EXPECT_FALSE(mt.get("dead").has_value());

    int live_count = 0;
    int tombstone_count = 0;
    mt.scan([&](const SkipListMemTable::EntryView& entry) {
        if (entry.type == ValueType::TypeDeletion) {
            ++tombstone_count;
        } else {
            ++live_count;
        }
    });

    EXPECT_GE(live_count, 2);
    EXPECT_GE(tombstone_count, 1);
}

TEST_F(SkipListMemTableTest, Resurrection) {
    ASSERT_EQ(mt.put("k", "v1", tlab), PutResult::Ok);
    ASSERT_EQ(mt.remove("k", tlab), PutResult::Ok);
    EXPECT_FALSE(mt.get("k").has_value());

    ASSERT_EQ(mt.put("k", "v2", tlab), PutResult::Ok);
    auto got = mt.get("k");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "v2");
}

TEST_F(SkipListMemTableTest, EmptyValueRoundTrip) {
    ASSERT_EQ(mt.put("k", "", tlab), PutResult::Ok);
    auto got = mt.get("k");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(got->size(), 0u);
}

TEST_F(SkipListMemTableTest, SingleByteKeyAndValue) {
    ASSERT_EQ(mt.put("x", "y", tlab), PutResult::Ok);
    auto got = mt.get("x");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, "y");
}

TEST(SkipListMemTable, SequenceNumbersAreUnique) {
    constexpr std::size_t kThreads = 8;
    constexpr std::size_t kPerThread = 512;

    auto arena = make_test_arena(256ULL << 20);
    auto memtable = make_test_memtable(arena);

    std::mutex mu;
    std::set<std::uint64_t> seqs;
    std::vector<std::thread> workers;
    workers.reserve(kThreads);

    for (std::size_t t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t] {
            TLAB local_tlab(arena);
            for (std::size_t i = 0; i < kPerThread; ++i) {
                const auto key = key_of(t * kPerThread + i);
                ASSERT_EQ(memtable.put(key, "v", local_tlab), PutResult::Ok);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    memtable.scan([&](const SkipListMemTable::EntryView& entry) {
        std::lock_guard lock(mu);
        seqs.insert(entry.sequence);
    });

    const std::size_t expected = kThreads * kPerThread;
    EXPECT_EQ(seqs.size(), expected) << "duplicate or missing sequence numbers";
}

TEST_F(SkipListMemTableTest, ScanNewestVersionFirstForDuplicates) {
    ASSERT_EQ(mt.put("k", "v1", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("k", "v2", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("k", "v3", tlab), PutResult::Ok);

    std::vector<std::pair<std::uint64_t, std::string>> entries;
    mt.scan([&](const SkipListMemTable::EntryView& entry) {
        if (entry.key == "k") {
            entries.emplace_back(entry.sequence, std::string(entry.value));
        }
    });

    ASSERT_EQ(entries.size(), 3u);

    for (std::size_t i = 1; i < entries.size(); ++i) {
        EXPECT_GT(entries[i - 1].first, entries[i].first)
            << "scan emitted versions out of sequence order at index " << i;
    }

    EXPECT_EQ(entries[0].second, "v3");
}

TEST(SkipListMemTable, LargeBatchScanIsSorted) {
    constexpr std::size_t kEntries = 4096;

    auto arena = make_test_arena(64ULL << 20);
    auto memtable = make_test_memtable(arena);
    TLAB tlab(arena);

    for (std::size_t i = kEntries; i-- > 0;) {
        ASSERT_EQ(memtable.put(key_of(i), "v", tlab), PutResult::Ok);
    }

    std::vector<std::string> keys_seen;
    keys_seen.reserve(kEntries);
    memtable.scan([&](const SkipListMemTable::EntryView& entry) { keys_seen.emplace_back(entry.key); });

    ASSERT_EQ(keys_seen.size(), kEntries);
    EXPECT_TRUE(std::is_sorted(keys_seen.begin(), keys_seen.end()));
}

TEST_F(SkipListMemTableTest, GetOnNeverInsertedKey) {
    ASSERT_EQ(mt.put("a", "1", tlab), PutResult::Ok);
    ASSERT_EQ(mt.put("c", "3", tlab), PutResult::Ok);

    EXPECT_FALSE(mt.get("b").has_value());
    EXPECT_FALSE(mt.get("").has_value());
    EXPECT_FALSE(mt.get("zzz").has_value());
}

TEST(SkipListMemTable, OomReturnNotCrash) {
    constexpr std::size_t kTinyArenaSize = 4096;

    auto arena = make_test_arena(kTinyArenaSize, 4096);
    auto memtable = make_test_memtable(arena, stratadb::helper::make_test_memtable_config(kTinyArenaSize));
    TLAB tlab(arena);

    PutResult last = PutResult::Ok;
    for (int i = 0; i < 10000; ++i) {
        last = memtable.put(key_of(static_cast<std::size_t>(i)), "v", tlab);
        if (last == PutResult::OutOfMemory) {
            break;
        }
    }

    EXPECT_EQ(last, PutResult::OutOfMemory);
    EXPECT_LE(memtable.memory_usage(), arena.capacity());

    EXPECT_NO_FATAL_FAILURE({
        auto res = memtable.get(key_of(0));
        if (res.has_value()) {
            EXPECT_EQ(*res, "v");
        }
    });
}

TEST(SkipListMemTable, FreshMemtableAfterArenaReset) {
    auto arena = make_test_arena();

    {
        auto memtable = make_test_memtable(arena);
        TLAB tlab(arena);
        ASSERT_EQ(memtable.put("old_key", "old_val", tlab), PutResult::Ok);
        EXPECT_TRUE(memtable.get("old_key").has_value());
    }

    arena.reset();

    {
        auto memtable = make_test_memtable(arena);
        TLAB tlab(arena);
        ASSERT_EQ(memtable.put("new_key", "new_val", tlab), PutResult::Ok);
        auto got = memtable.get("new_key");
        ASSERT_TRUE(got.has_value());
        EXPECT_EQ(*got, "new_val");
        EXPECT_FALSE(memtable.get("old_key").has_value());
    }
}

TEST_F(SkipListMemTableTest, TlabDetachPreventsAllocation) {
    ASSERT_EQ(mt.put("before", "v", tlab), PutResult::Ok);

    tlab.detach();
    EXPECT_FALSE(tlab.is_attached());

    EXPECT_EQ(mt.put("after", "v", tlab), PutResult::OutOfMemory);
    EXPECT_TRUE(mt.get("before").has_value());
}

TEST(SkipListMemTable, ConcurrentAllKeysVisible) {
    constexpr std::size_t kThreads = 16;
    constexpr std::size_t kPerThread = 512;

    auto arena = make_test_arena(256ULL << 20);
    auto memtable = make_test_memtable(arena);

    std::vector<std::thread> workers;
    workers.reserve(kThreads);

    for (std::size_t t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t] {
            TLAB tlab(arena);
            for (std::size_t i = 0; i < kPerThread; ++i) {
                const auto key = key_of(t * kPerThread + i);
                const auto value = "val_" + std::to_string(t * kPerThread + i);
                ASSERT_EQ(memtable.put(key, value, tlab), PutResult::Ok);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    for (std::size_t t = 0; t < kThreads; ++t) {
        for (std::size_t i = 0; i < kPerThread; ++i) {
            const auto key = key_of(t * kPerThread + i);
            auto got = memtable.get(key);
            ASSERT_TRUE(got.has_value()) << "key " << key << " not found after concurrent insert";
        }
    }
}

TEST(SkipListMemTable, ConcurrentPutAndScanNoCrash) {
    auto arena = make_test_arena(64ULL << 20);
    auto memtable = make_test_memtable(arena);

    std::atomic<bool> done_writing{false};

    std::thread writer([&] {
        TLAB tlab(arena);
        for (std::size_t i = 0; i < 2048; ++i) {
            ASSERT_EQ(memtable.put(key_of(i), "v", tlab), PutResult::Ok);
        }
        done_writing.store(true, std::memory_order_release);
    });

    std::thread scanner([&] {
        while (!done_writing.load(std::memory_order_acquire)) {
            std::size_t count = 0;
            memtable.scan([&count](const SkipListMemTable::EntryView&) { ++count; });
            do_not_optimize(count);
        }

        std::size_t final_count = 0;
        memtable.scan([&final_count](const SkipListMemTable::EntryView&) { ++final_count; });
        do_not_optimize(final_count);
    });

    writer.join();
    scanner.join();
}

TEST(SkipListMemTable, ConcurrentSameKeyspaceNoCrash) {
    constexpr std::size_t kThreads = 8;
    constexpr std::size_t kKeySpace = 256;
    constexpr std::size_t kOpsEach = 1024;

    auto arena = make_test_arena(64ULL << 20);
    auto memtable = make_test_memtable(arena);

    std::vector<std::thread> workers;
    workers.reserve(kThreads);

    for (std::size_t t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t] {
            TLAB tlab(arena);
            for (std::size_t op = 0; op < kOpsEach; ++op) {
                const auto key = key_of(op % kKeySpace);
                const auto value = "t" + std::to_string(t) + "_op" + std::to_string(op);
                const auto res = memtable.put(key, value, tlab);
                EXPECT_TRUE(res == PutResult::Ok || res == PutResult::FlushNeeded || res == PutResult::StallNeeded)
                    << "unexpected put result: " << static_cast<int>(res);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    for (std::size_t k = 0; k < kKeySpace; ++k) {
        const auto key = key_of(k);
        auto got = memtable.get(key);
        ASSERT_TRUE(got.has_value()) << "key " << key << " missing after concurrent same-keyspace inserts";
        EXPECT_FALSE(got->empty());
        EXPECT_EQ((*got)[0], 't') << "value corruption detected for key " << key << ": " << *got;
    }
}
