
#include "../support/test_config.hpp"
#include "stratadb/config/immutable/memory_config.hpp"
#include "stratadb/config/immutable/page_config.hpp"
#include "stratadb/memory/virtual_region.hpp"
#include "stratadb/utils/probe.hpp"

#include <cstddef>
#include <cstring>
#include <gtest/gtest.h>

using namespace stratadb::memory;
using namespace stratadb::config;

namespace {} // namespace

// Tests

TEST(VirtualRegion, BasicAllocation) {
    constexpr std::size_t kSize = 4ULL * 1024 * 1024; // 4 MiB
    auto result = VirtualRegion::allocate(stratadb::test::test_memory_config(kSize));

    ASSERT_TRUE(result.has_value()) << "VirtualRegion::allocate returned error";
    EXPECT_NE(result->data(), nullptr);
    EXPECT_EQ(result->size(), kSize);
}

TEST(VirtualRegion, PageAlignment) {
    const std::size_t page_sz = stratadb::utils::system_page_size();
    constexpr std::size_t kSize = 8ULL * 1024 * 1024;

    auto result = VirtualRegion::allocate(stratadb::test::test_memory_config(kSize));
    ASSERT_TRUE(result.has_value());

    const auto addr = reinterpret_cast<std::uintptr_t>(result->data());
    EXPECT_EQ(addr % page_sz, 0u) << "Region base is not page-aligned";
}

TEST(VirtualRegion, ReadAndWrite) {
    constexpr std::size_t kSize = 4ULL * 1024 * 1024;
    auto result = VirtualRegion::allocate(stratadb::test::test_memory_config(kSize));
    ASSERT_TRUE(result.has_value());

    std::byte* base = result->data();

    // Write a pattern across the entire region.
    std::memset(base, 0xAB, kSize);

    // Verify at a few spots.
    EXPECT_EQ(base[0], std::byte{0xAB});
    EXPECT_EQ(base[kSize / 2], std::byte{0xAB});
    EXPECT_EQ(base[kSize - 1], std::byte{0xAB});
}

TEST(VirtualRegion, MoveSemantics) {
    constexpr std::size_t kSize = 4ULL * 1024 * 1024;
    auto result = VirtualRegion::allocate(stratadb::test::test_memory_config(kSize));
    ASSERT_TRUE(result.has_value());

    VirtualRegion a = std::move(*result);
    ASSERT_NE(a.data(), nullptr);

    VirtualRegion b = std::move(a);
    EXPECT_NE(b.data(), nullptr);
    EXPECT_EQ(a.data(), nullptr); // moved-from is inert
    EXPECT_EQ(b.size(), kSize);
}

TEST(VirtualRegion, MoveAssignment) {
    constexpr std::size_t kSize = 4ULL * 1024 * 1024;

    auto r1 = VirtualRegion::allocate(stratadb::test::test_memory_config(kSize));
    auto r2 = VirtualRegion::allocate(stratadb::test::test_memory_config(kSize));
    ASSERT_TRUE(r1.has_value());
    ASSERT_TRUE(r2.has_value());

    std::byte* orig_ptr = r2->data();
    *r1 = std::move(*r2);

    EXPECT_EQ(r1->data(), orig_ptr);
    EXPECT_EQ(r2->data(), nullptr);
}

TEST(VirtualRegion, HugePagesOpportunisticDoesNotCrash) {
    constexpr std::size_t kSize = 8ULL * 1024 * 1024;
    // May fall back to standard 4K if hugepages are unavailable.
    auto cfg = stratadb::test::test_memory_config(kSize);
    cfg.page_strategy = PageStrategy::Huge2M_Opportunistic;
    auto result = VirtualRegion::allocate(cfg);

    // Either succeeds with 2M pages or silently falls back to 4K.
    EXPECT_TRUE(result.has_value()) << "Opportunistic huge-page allocation should never fail outright";

    if (result.has_value()) {
        EXPECT_NE(result->data(), nullptr);
        EXPECT_EQ(result->size(), kSize);
    }
}

TEST(VirtualRegion, PrefaultDoesNotCrash) {
    constexpr std::size_t kSize = 4ULL * 1024 * 1024;
    auto cfg = stratadb::test::test_memory_config(kSize);
    cfg.page_strategy = PageStrategy::Standard4K;
    cfg.prefault_on_init = true;
    auto result = VirtualRegion::allocate(cfg);

    ASSERT_TRUE(result.has_value());
    EXPECT_NE(result->data(), nullptr);
}

TEST(VirtualRegion, NumaInterleavedDoesNotCrash) {
    constexpr std::size_t kSize = 4ULL * 1024 * 1024;
    // On non-NUMA systems mbind may fail gracefully; the allocator falls back.
    auto cfg = stratadb::test::test_memory_config(kSize);
    cfg.page_strategy = PageStrategy::Standard4K;
    cfg.numa_policy = NumaPolicy::Interleaved;

    cfg.prefault_on_init = false;

    auto result = VirtualRegion::allocate(cfg);

    // On systems without NUMA, VirtualRegion::allocate may still succeed
    // (mbind failure for Interleaved is not fatal).
    if (result.has_value()) {
        EXPECT_NE(result->data(), nullptr);
    } else {
        // MbindFailed is the expected error on strict non-NUMA systems.
        EXPECT_EQ(result.error(), ArenaError::MbindFailed);
    }
}

TEST(VirtualRegion, ActualPageStrategyReflectsReality) {
    constexpr std::size_t kSize = 4ULL * 1024 * 1024;

    auto cfg = stratadb::test::test_memory_config(kSize);
    cfg.page_strategy = PageStrategy::Standard4K;
    auto result = VirtualRegion::allocate(cfg);

    ASSERT_TRUE(result.has_value());
    // Standard4K never degrades; it must always match.
    EXPECT_EQ(result->actual_page_strategy(), PageStrategy::Standard4K);
}

TEST(VirtualRegion, ZeroSizeDefaultConstruct) {
    // Default-constructed region is inert and safe to destroy.
    VirtualRegion r;
    EXPECT_EQ(r.data(), nullptr);
    EXPECT_EQ(r.size(), 0u);
    // Destructor must not crash (should not call munmap(nullptr, 0)).
}
