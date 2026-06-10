
#include "stratadb/platform/hardware_model.hpp"
#include "stratadb/platform/hardware_probe.hpp"
#include "stratadb/utils/os.hpp"
#include "stratadb/utils/probe.hpp"

#include <bit>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>

using namespace stratadb;

// utils/probe.hpp

TEST(HardwareProbe, LogicalCoreCountSane) {
    const auto count = utils::logical_core_count();
    EXPECT_GE(count, 1u) << "System must have at least one logical core";
    EXPECT_LE(count, 65536u) << "Implausibly large core count";
}

TEST(HardwareProbe, SystemPageSizeIsPowerOfTwo) {
    const auto sz = utils::system_page_size();
    EXPECT_GE(sz, 4096u);
    EXPECT_TRUE(std::has_single_bit(sz)) << "Page size must be a power of two, got " << sz;
}

TEST(HardwareProbe, TotalPhysicalMemorySane) {
    const auto mem = utils::total_physical_memory_bytes();
    // On Linux and macOS this should return something sensible.
    // On unsupported platforms it returns 0.
#if defined(__linux__) || defined(__APPLE__)
    EXPECT_GT(mem, 0u) << "total_physical_memory_bytes() returned 0";
    EXPECT_GE(mem, 64u * 1024 * 1024) << "Less than 64 MiB? Unexpected.";
#else
    // Platform may not expose this — just ensure no crash.
    (void)mem;
    SUCCEED();
#endif
}

TEST(HardwareProbe, SectorProbesReturnSafeDefaultsOnBadFd) {
    // fd = -1 triggers the fallback branch in each probe.
    const auto lss = utils::probe_logical_sector_size(-1);
    const auto pss = utils::probe_physical_sector_size(-1);
    const auto awmin = utils::probe_atomic_write_unit_min(-1);
    const auto awmax = utils::probe_atomic_write_unit_max(-1);

    EXPECT_EQ(lss, 512u) << "Default logical sector size must be 512";
    EXPECT_EQ(pss, 4096u) << "Default physical sector size must be 4096";
    EXPECT_GE(awmin, 512u);
    EXPECT_GE(awmax, awmin);
}

TEST(HardwareProbe, RotationalDefaultIsFalse) {
    // On an unknown fd the safe default for is_rotational is false (assume SSD).
    EXPECT_FALSE(utils::probe_is_rotational(-1));
}

TEST(HardwareProbe, SupportsFallocateOnLinux) {
#if defined(__linux__)
    EXPECT_TRUE(utils::probe_supports_fallocate(-1));
#else
    EXPECT_FALSE(utils::probe_supports_fallocate(-1));
#endif
}

// utils/os.hpp — CPU isolation helpers

TEST(HardwareProbe, AutoDiscoverIsolatedCoreDoesNotCrash) {
    // May return nullopt on systems with no isolated CPUs — that's correct.
    auto result = utils::os::auto_discover_isolated_core();
    if (result.has_value()) {
        EXPECT_LT(*result, utils::logical_core_count()) << "Discovered core id must be < logical_core_count()";
    }
    SUCCEED();
}

TEST(HardwareProbe, IsCoreIsolatedOutOfBoundsReturnsFalse) {
    // Core 999999 cannot be isolated on any real machine.
    EXPECT_FALSE(utils::os::is_core_isolated(999999u));
}

// platform/hardware_probe.hpp

TEST(HardwareProbe, ProbeHardwareNonExistentPathUsesDefaults) {
    platform::HardwareOverrides overrides{};
    // Non-existent directory — open() fails, fd=-1, all probes fall back.
    auto info = platform::probe_hardware("/nonexistent/stratadb_test_dir", overrides);

    EXPECT_GE(info.cpu.logical_count, 1u);
    EXPECT_GE(info.memory.page_size_bytes, 4096u);
    // IO fields must have safe non-zero defaults.
    EXPECT_GE(info.io.logical_sector_size, 512u);
    EXPECT_GE(info.io.physical_sector_size, 512u);
}

TEST(HardwareProbe, ProbeHardwareRealDirReturnsValidValues) {
    platform::HardwareOverrides overrides{};
    auto info = platform::probe_hardware("/tmp", overrides);

    EXPECT_GE(info.cpu.logical_count, 1u);
    EXPECT_GE(info.memory.page_size_bytes, 4096u);
    EXPECT_GE(info.io.logical_sector_size, 512u);
}

TEST(HardwareProbe, OverridesWinOverOsValues) {
    platform::HardwareOverrides overrides{};
    overrides.cpu.logical_count = 42u;
    overrides.io.is_rotational = true;

    auto info = platform::probe_hardware("/tmp", overrides);

    EXPECT_EQ(info.cpu.logical_count, 42u) << "Override for logical_count must win over OS probe";
    EXPECT_TRUE(info.io.is_rotational) << "Override for is_rotational must win over OS probe";
}

// Internal parser smoke test via the observable surface
//
// We cannot call detail::parse_cpu_ranges() directly (it's in an anonymous
// namespace in os.cpp).  Instead, we write a known isolated-cpu file to a
// temp location and re-parse it via auto_discover_isolated_core() ...
// except that function reads from the hard-coded sysfs path.
//
// The practical alternative: just assert that the function behaves correctly
// on this machine and doesn't crash with weird outputs.

TEST(HardwareProbe, IsolateCpuParserDoesNotCrashOnRealSystem) {
    // Run auto_discover twice — must be idempotent and not crash.
    auto r1 = utils::os::auto_discover_isolated_core();
    auto r2 = utils::os::auto_discover_isolated_core();

    EXPECT_EQ(r1.has_value(), r2.has_value());
    if (r1.has_value() && r2.has_value()) {
        EXPECT_EQ(*r1, *r2);
    }
}