#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/utils/hardware.hpp"

#include <gtest/gtest.h>
#include <random>
#include <vector>

using namespace stratadb::memory;
using namespace stratadb::config;

// helper
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static auto make_config(std::size_t total, std::size_t tlab) {
    MemoryConfig cfg;
    cfg.total_budget_bytes = total;
    cfg.tlab_size_bytes = tlab;
    return cfg;
}

// ---------- BASIC ----------

TEST(TLAB, BasicAllocation) {
    auto arena = Arena::create(make_config(10ULL * 1024 * 1024, 2ULL * 1024 * 1024)).value();
    TLAB tlab(arena);

    auto ptr = tlab.allocate(64);

    EXPECT_NE(ptr, nullptr);
}

// ---------- ALIGNMENT ----------

TEST(TLAB, Alignment) {
    auto arena = Arena::create(make_config(10ULL * 1024 * 1024, 2ULL * 1024 * 1024)).value();
    TLAB tlab(arena);

    void* ptr = tlab.allocate(15, 64);

    ASSERT_NE(ptr, nullptr);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(ptr) % 64, 0u);
}

// ---------- SEQUENTIAL (NO OVERLAP) ----------

TEST(TLAB, SequentialAllocation) {
    auto arena = Arena::create(make_config(10ULL * 1024 * 1024, 2ULL * 1024 * 1024)).value();
    TLAB tlab(arena);

    std::vector<std::uintptr_t> ptrs;

    for (std::size_t i = 0; i < 10; ++i) {
        auto p = reinterpret_cast<std::uintptr_t>(tlab.allocate(64));
        ASSERT_NE(p, 0u);
        ptrs.push_back(p);
    }

    for (std::size_t i = 1; i < ptrs.size(); ++i) {
        EXPECT_GT(ptrs[i], ptrs[i - 1]);
    }
}

// ---------- REFILL ----------

TEST(TLAB, RefillTrigger) {
    auto arena = Arena::create(make_config(16ULL * 1024, 4ULL * 1024)).value();
    TLAB tlab(arena);

    std::vector<void*> ptrs;

    for (std::size_t i = 0; i < 10; ++i) {
        auto p = tlab.allocate(1024);
        ASSERT_NE(p, nullptr);
        ptrs.push_back(p);
    }

    // Ensure pointers are still valid and increasing
    for (std::size_t i = 1; i < ptrs.size(); ++i) {
        EXPECT_NE(ptrs[i], ptrs[i - 1]);
    }
}

TEST(TLAB, RefillsForSmallAllocationWhenTinySlackRemains) {
    auto arena = Arena::create(make_config(16ULL * 1024, 4ULL * 1024)).value();
    TLAB tlab(arena);
    const std::size_t expected_alignment = stratadb::utils::system_page_size();

    for (std::size_t i = 0; i < 127; ++i) {
        ASSERT_NE(tlab.allocate(32, 8), nullptr);
    }

    EXPECT_EQ(arena.memory_used(), expected_alignment);

    ASSERT_NE(tlab.allocate(64, 8), nullptr);
    EXPECT_EQ(arena.memory_used(), expected_alignment * 2);

    ASSERT_NE(tlab.allocate(64, 8), nullptr);
    EXPECT_EQ(arena.memory_used(), expected_alignment * 2);
}

// ---------- EXACT BOUNDARY ----------

TEST(TLAB, ExactBoundary) {
    auto arena = Arena::create(make_config(8192, MemoryConfig::DEFAULT_BLOCK_ALIGNMENT)).value();
    TLAB tlab(arena);

    auto p1 = tlab.allocate(2048);
    auto p2 = tlab.allocate(2048);

    ASSERT_NE(p1, nullptr);
    ASSERT_NE(p2, nullptr);

    // next allocation must trigger refill
    auto p3 = tlab.allocate(2048);

    EXPECT_NE(p3, nullptr);
}

// ---------- LARGE ALLOCATION ----------

TEST(TLAB, LargeAllocation) {
    auto arena = Arena::create(make_config(16ULL * 1024 * 1024, 2ULL * 1024 * 1024)).value();
    TLAB tlab(arena);

    auto p = tlab.allocate(5ULL * 1024 * 1024);

    ASSERT_NE(p, nullptr);
}

// ---------- OOM ----------

TEST(TLAB, OutOfMemory) {
    auto arena = Arena::create(make_config(8192, MemoryConfig::DEFAULT_BLOCK_ALIGNMENT)).value();
    TLAB tlab(arena);

    while (true) {
        if (tlab.allocate(1024) == nullptr)
            break;
    }

    auto p = tlab.allocate(1024);
    EXPECT_EQ(p, nullptr);
}

// ---------- RANDOM STRESS ----------

TEST(TLAB, RandomStress) {
    auto arena = Arena::create(make_config(32ULL * 1024 * 1024, 2ULL * 1024 * 1024)).value();
    TLAB tlab(arena);

    std::mt19937 rng(42);
    std::uniform_int_distribution<std::size_t> dist(1, 1 << 20);

    for (std::size_t i = 0; i < 10000; ++i) {
        auto p = tlab.allocate(dist(rng));
        if (!p)
            break;
    }

    EXPECT_LE(arena.memory_used(), arena.capacity());
}

// ---------- ALIGNMENT TORTURE ----------

TEST(TLAB, AlignmentTorture) {
    auto arena = Arena::create(make_config(32ULL * 1024 * 1024, 2ULL * 1024 * 1024)).value();
    TLAB tlab(arena);

    std::vector<std::size_t> aligns = {8, 16, 32, 64, 128, 256};

    for (auto a : aligns) {
        auto p = tlab.allocate(64, a);
        ASSERT_NE(p, nullptr);
        EXPECT_EQ(reinterpret_cast<std::uintptr_t>(p) % a, 0u);
    }
}
