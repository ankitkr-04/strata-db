#include "stratadb/memory/arena.hpp"

#include <atomic>
#include <gtest/gtest.h>
#include <random>
#include <thread>
#include <vector>

using namespace stratadb::memory;
using namespace stratadb::config;

// ---------- helpers ----------

static auto make_config(std::size_t total,
                        PageStrategy strategy = PageStrategy::Standard4K,
                        std::size_t tlab = 2ULL * 1024 * 1024) {
    MemoryConfig cfg;
    cfg.total_budget_bytes = total;
    cfg.tlab_size_bytes = tlab;
    cfg.page_strategy = strategy;
    return cfg;
}

// ---------- tests ----------

TEST(Arena, Initialization) {
    auto cfg = make_config(10ULL * 1024 * 1024);

    auto arena_exp = Arena::create(cfg);
    ASSERT_TRUE(arena_exp.has_value());

    auto& arena = arena_exp.value();

    EXPECT_EQ(arena.memory_used(), 0u);
    EXPECT_EQ(arena.capacity(), cfg.total_budget_bytes);
    EXPECT_EQ(arena.remaining(), cfg.total_budget_bytes);
}

TEST(Arena, AllocateBlockBasic) {
    auto cfg = make_config(10ULL * 1024 * 1024);

    auto arena = Arena::create(cfg).value();

    auto span = arena.allocate_block(128);

    ASSERT_FALSE(span.empty());
    EXPECT_EQ(span.size(), 2 * 1024 * 1024); // tlab size
    EXPECT_EQ(arena.memory_used(), span.size());
    EXPECT_EQ(arena.remaining(), arena.capacity() - arena.memory_used());
}

TEST(Arena, AllocateBlockLarge) {
    auto cfg = make_config(20ULL * 1024 * 1024);

    auto arena = Arena::create(cfg).value();

    std::size_t req = 5ULL * 1024 * 1024;

    auto span = arena.allocate_block(req);

    ASSERT_FALSE(span.empty());
    EXPECT_GE(span.size(), req);
    EXPECT_EQ(span.size() % cfg.block_alignment_bytes, 0u);
}

TEST(Arena, Alignment) {
    auto cfg = make_config(10ULL * 1024 * 1024);

    auto arena = Arena::create(cfg).value();

    auto span = arena.allocate_block(128);

    ASSERT_FALSE(span.empty());

    auto ptr = reinterpret_cast<std::uintptr_t>(span.data());
    EXPECT_EQ(ptr % cfg.block_alignment_bytes, 0u);
}

TEST(Arena, OutOfMemory) {
    auto cfg = make_config(8ULL * 1024 * 1024);

    auto arena = Arena::create(cfg).value();

    std::vector<std::span<std::byte>> blocks;

    while (true) {
        auto s = arena.allocate_block(cfg.tlab_size_bytes);
        if (s.empty())
            break;
        blocks.push_back(s);
    }

    auto final = arena.allocate_block(cfg.tlab_size_bytes);

    EXPECT_TRUE(final.empty());
    EXPECT_EQ(final.data(), nullptr);
}

TEST(Arena, Reset) {
    auto cfg = make_config(10ULL * 1024 * 1024);

    auto arena = Arena::create(cfg).value();

    auto s1 = arena.allocate_block(1024);
    auto s2 = arena.allocate_block(1024);

    EXPECT_FALSE(s1.empty());
    EXPECT_FALSE(s2.empty());

    EXPECT_GT(arena.memory_used(), 0u);

    arena.reset();

    EXPECT_EQ(arena.memory_used(), 0u);

    auto s = arena.allocate_block(1024);
    EXPECT_FALSE(s.empty());
}
TEST(Arena, MoveSemantics) {
    auto cfg = make_config(10ULL * 1024 * 1024);

    auto a = Arena::create(cfg).value();

    auto s = a.allocate_block(1024);

    ASSERT_FALSE(s.empty());

    auto used_before = a.memory_used();

    Arena b = std::move(a);

    EXPECT_EQ(b.memory_used(), used_before);

    // moved-from should fail safely
    auto s2 = a.allocate_block(1024);
    EXPECT_TRUE(s2.empty());
}

TEST(Arena, NoOverlap) {
    auto cfg = make_config(20ULL * 1024 * 1024);

    auto arena = Arena::create(cfg).value();

    std::vector<std::uintptr_t> ptrs;

    for (std::size_t i = 0; i < 5; ++i) {
        auto s = arena.allocate_block(1024);
        ASSERT_FALSE(s.empty());

        ptrs.push_back(reinterpret_cast<std::uintptr_t>(s.data()));
    }

    for (std::size_t i = 0; i < ptrs.size(); ++i) {
        for (std::size_t j = i + 1; j < ptrs.size(); ++j) {
            EXPECT_NE(ptrs[i], ptrs[j]);
        }
    }
}

TEST(Arena, NearBoundary) {
    auto cfg = make_config(4ULL * 1024 * 1024);

    auto arena = Arena::create(cfg).value();

    auto s1 = arena.allocate_block(3ULL * 1024 * 1024);
    ASSERT_FALSE(s1.empty());

    auto s2 = arena.allocate_block(2ULL * 1024 * 1024);

    EXPECT_TRUE(s2.empty());
}

TEST(Arena, ConcurrentAllocation) {
    auto cfg = make_config(512ULL * 1024 * 1024);

    auto arena = Arena::create(cfg).value();

    constexpr std::size_t threads = 4;
    constexpr std::size_t iters = 50;

    std::vector<std::thread> workers;
    std::atomic<bool> failed = false;

    for (std::size_t t = 0; t < threads; ++t) {
        workers.emplace_back([&]() {
            for (std::size_t i = 0; i < iters; ++i) {
                auto s = arena.allocate_block(1024);
                if (s.empty()) {
                    failed = true;
                    return;
                }
            }
        });
    }

    for (auto& th : workers)
        th.join();

    EXPECT_FALSE(failed);
}

TEST(Arena, RandomStress) {
    auto cfg = make_config(64ULL * 1024 * 1024);

    auto arena = Arena::create(cfg).value();

    std::mt19937 rng(42);
    std::uniform_int_distribution<std::size_t> dist(1, 1 << 20);

    for (std::size_t i = 0; i < 1000; ++i) {
        auto s = arena.allocate_block(dist(rng));
        if (s.empty())
            break;
    }

    EXPECT_LE(arena.memory_used(), arena.capacity());
}

TEST(Arena, NumaPolicyDoesNotCrash) {
    MemoryConfig cfg;
    cfg.total_budget_bytes = 16ULL * 1024 * 1024;
    cfg.numa_policy = NumaPolicy::Interleaved;

    auto arena = Arena::create(cfg);

    // On non-NUMA systems, this should still succeed
    EXPECT_TRUE(arena.has_value());
}

TEST(Arena, PrefaultDoesNotCrash) {
    MemoryConfig cfg;
    cfg.total_budget_bytes = 32ULL * 1024 * 1024;
    cfg.prefault_on_init = true;

    auto arena = Arena::create(cfg);

    EXPECT_TRUE(arena.has_value());
}
