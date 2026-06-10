#pragma once

// Shared infrastructure for WAL, segment-pool, and IO tests.

#include "stratadb/config/config_manager.hpp"
#include "stratadb/config/config_resolver.hpp"
#include "stratadb/config/immutable_config.hpp"
#include "stratadb/config/mutable_config.hpp"
#include "stratadb/memory/epoch_manager.hpp"
#include "stratadb/platform/hardware_model.hpp"
#include "stratadb/platform/identity.hpp"
#include "stratadb/utils/os.hpp"
#include "stratadb/wal/block/gamma_block.hpp"
#include "stratadb/wal/manager.hpp"
#include "stratadb/wal/reader/validator.hpp"
#include "stratadb/wal/slot/slot_footer.hpp"
#include "stratadb/wal/slot/slot_header.hpp"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <unistd.h>
#include <vector>

namespace stratadb::wal::test {

using WalRecord = std::pair<std::string, std::string>;
using WalBlockRecords = std::vector<WalRecord>;
using WalSegmentBlocks = std::vector<WalBlockRecords>;

// TempDir

struct TempDir {
    std::filesystem::path path;

    TempDir() {
        for (const char* base : {"/var/tmp", "/tmp"}) {
            char buf[256];
            std::snprintf(buf, sizeof(buf), "%s/stratadb_test_XXXXXX", base);
            if (char* result = ::mkdtemp(buf)) {
                path = result;
                return;
            }
        }
        throw std::runtime_error("mkdtemp failed");
    }

    ~TempDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }

    TempDir(const TempDir&) = delete;
    auto operator=(const TempDir&) -> TempDir& = delete;
    TempDir(TempDir&&) noexcept = default;
    auto operator=(TempDir&&) noexcept -> TempDir& = default;
};

// O_DIRECT probe

// Returns false if O_DIRECT is unsupported on the given directory's filesystem
// (common on tmpfs mounts). Tests that rely on the segment pool or WalReader
// should call this and GTEST_SKIP() when it returns false.
inline auto supports_odirect(const std::filesystem::path& dir) -> bool {
    auto probe_path = dir / ".odirect_probe";

    // Create a minimal file via buffered I/O
    auto buf = utils::os::open_buffered(probe_path, /*create=*/true);
    if (!buf.has_value())
        return false;
    ::posix_fallocate(*buf, 0, 4096);
    if (!utils::os::sync_data(*buf)) {
    }

    utils::os::close_fd(*buf);

    // Try to re-open with O_DIRECT
    auto direct = utils::os::open_direct(probe_path);
    bool ok = direct.has_value();
    if (ok)
        utils::os::close_fd(*direct);

    std::error_code ec;
    std::filesystem::remove(probe_path, ec);
    return ok;
}

#define WAL_SKIP_IF_NO_ODIRECT(dir_path)                                                                               \
    do {                                                                                                               \
        if (!::stratadb::wal::test::supports_odirect(dir_path)) {                                                      \
            GTEST_SKIP() << "O_DIRECT not supported on this filesystem";                                               \
        }                                                                                                              \
    } while (false)

// Hardware / identity factories

inline auto make_test_hw_info() -> platform::HardwareInfo {
    platform::HardwareInfo hw{};
    hw.cpu.logical_count = 4;
    hw.memory.total_bytes = 1UL << 30;  // 1 GiB
    hw.memory.page_size_bytes = 4096;
    hw.io.logical_sector_size = 512;
    hw.io.physical_sector_size = 4096;  // standard NVMe
    hw.io.atomic_write_unit_min = 512;
    hw.io.atomic_write_unit_max = 4096;
    hw.io.is_rotational = false;
    hw.io.supports_fua = false;
    hw.io.supports_rwf_atomic = false;
    hw.io.supports_fallocate = true;
    return hw;
}

inline auto make_test_hw_info_hdd() -> platform::HardwareInfo {
    auto hw = make_test_hw_info();
    hw.io.is_rotational = true;
    hw.io.supports_fallocate = true;
    return hw;
}

inline auto make_test_hw_info_16k() -> platform::HardwareInfo {
    auto hw = make_test_hw_info();
    hw.io.physical_sector_size = 16384;  // enterprise NVMe
    return hw;
}

inline auto make_test_identity() -> platform::DbIdentity {
    platform::DbIdentity id{};
    for (std::size_t i = 0; i < 16; ++i)
        id.bytes[i] = static_cast<std::uint8_t>(i + 1);
    // Version 4 + variant bits
    id.bytes[6] = static_cast<std::uint8_t>((id.bytes[6] & 0x0F) | 0x40);
    id.bytes[8] = static_cast<std::uint8_t>((id.bytes[8] & 0x3F) | 0x80);
    return id;
}

inline auto make_test_uuid() -> std::array<std::uint8_t, 16> {
    return make_test_identity().bytes;
}

// WalManagerFixture

// Slot size used by all WAL manager tests: 256 KiB keeps tests fast while
// leaving enough room for many 4 KiB blocks.
inline constexpr std::uint64_t kTestSlotSizeBytes = static_cast<const std::uint64_t>(256 * 1024);

struct WalManagerFixture : ::testing::Test {
    TempDir wal_dir{};

    // EpochManager must be declared before the registration guard and before
    // ConfigManager (both depend on it at construction time).
    memory::EpochManager epoch_mgr{};
    memory::EpochManager::ThreadRegistrationGuard epoch_reg{epoch_mgr};

    config::ImmutableConfig imm_cfg = [] {
        config::ImmutableConfig cfg{};
        // Smaller pool for tests: 64 blocks x 32 KiB = 2 MiB.
        cfg.block_pool.capacity = 64;
        cfg.block_pool.payload_alignment_bytes = 4096;
        auto resolved = config::ConfigResolver::resolve_immutable(cfg);
        return resolved.value_or(cfg);
    }();

    config::MutableConfig mut_cfg{};
    config::ConfigManager config_mgr{imm_cfg, mut_cfg, epoch_mgr};

    platform::HardwareInfo hw_info{make_test_hw_info()};
    platform::DbIdentity identity{make_test_identity()};

    // Build a minimal WalConfig suitable for unit tests.
    [[nodiscard]] auto make_wal_cfg() const -> config::WalConfig {
        config::WalConfig cfg{};
        cfg.slot_size_bytes = kTestSlotSizeBytes;
        cfg.preallocated_pool_size = 1;  // pool_capacity becomes 3
        cfg.spsc.mode = config::SpscMode::Disabled;
        return cfg;
    }

    // Convenience: construct a WalManager with a specific (or default) config.
    [[nodiscard]] auto make_wal(config::WalConfig wal_cfg = {}) -> std::unique_ptr<WalManager> {
        if (wal_cfg.slot_size_bytes == 0)
            wal_cfg = make_wal_cfg();
        return std::make_unique<WalManager>(
            wal_cfg, imm_cfg.block_pool, config_mgr, epoch_mgr, wal_dir.path, hw_info, identity);
    }
};

inline auto segment_path(const std::filesystem::path& dir, std::uint64_t seq) -> std::filesystem::path {
    char name[32];
    std::snprintf(name, sizeof(name), "wal_%012llu.log", static_cast<unsigned long long>(seq));
    return dir / name;
}

inline void create_sealed_wal_file(const std::filesystem::path& dir,
                                   std::uint64_t seq,
                                   std::uint64_t slot_size,
                                   const std::array<std::uint8_t, 16>& uuid,
                                   reader::BlockLayout layout,
                                   std::uint64_t max_lsn = 0) {
    using slot::WalSlotFooter;
    using slot::WalSlotHeader;

    auto path = segment_path(dir, seq);
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(fd, 0) << "Failed to create " << path;

    WalSlotHeader hdr{};
    hdr.magic = WalSlotHeader::MAGIC;
    hdr.version = WalSlotHeader::VERSION;
    hdr.block_layout = static_cast<std::uint8_t>(layout);
    hdr.slot_sequence = seq;
    hdr.slot_max_bytes = slot_size;
    hdr.db_instance_uuid = uuid;
    slot::seal_header_crc(hdr);
    ASSERT_EQ(::pwrite(fd, &hdr, sizeof(hdr), 0), static_cast<ssize_t>(sizeof(hdr)));

    ASSERT_EQ(::posix_fallocate(fd, 0, static_cast<ssize_t>(static_cast<off_t>(slot_size))), 0);

    WalSlotFooter footer{};
    footer.magic = WalSlotFooter::MAGIC;
    footer.slot_sequence = seq;
    footer.sealed_write_offset = WalSlotHeader::SIZE; // no user data
    footer.min_lsn = (max_lsn == 0 ? 0 : max_lsn - 1);
    footer.max_lsn = max_lsn;
    slot::seal_footer_crc(footer);
    const off_t footer_off = static_cast<off_t>(slot_size - WalSlotFooter::SIZE);
    ASSERT_EQ(::pwrite(fd, &footer, sizeof(footer), footer_off), static_cast<ssize_t>(sizeof(footer)));

    ASSERT_EQ(::fdatasync(fd), 0);
    ::close(fd);
}

inline auto write_gamma_block_to_fd(int fd,
                                    std::uint64_t file_offset,
                                    std::uint64_t lsn,
                                    const WalBlockRecords& records) -> std::uint64_t {

    alignas(4096) GammaBlock<4096> block{};
    block.init(lsn);
    for (const auto& [k, v] : records) {
        bool ok = block.append({reinterpret_cast<const std::byte*>(k.data()), k.size()},
                               {reinterpret_cast<const std::byte*>(v.data()), v.size()});
        (void)ok;
    }
    auto result = block.finalize(lsn);

    const auto* data = result.memory_to_write.data();
    const std::size_t len = result.memory_to_write.size();
    EXPECT_EQ(::pwrite(fd, data, len, static_cast<off_t>(file_offset + result.block_internal_offset)),
              static_cast<ssize_t>(len));

    return file_offset + 4096;
}

inline auto make_sealed_segment(const std::filesystem::path& dir,
                                std::uint64_t seq,
                                std::uint64_t slot_size,
                                const std::array<std::uint8_t, 16>& uuid,
                                const WalSegmentBlocks& blocks_data) -> std::uint64_t {
    using slot::WalSlotFooter;
    using slot::WalSlotHeader;

    auto path = segment_path(dir, seq);
    int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        ADD_FAILURE() << "Failed to create " << path;
        return 0;
    }

    EXPECT_EQ(::posix_fallocate(fd, 0, static_cast<off_t>(slot_size)), 0);

    WalSlotHeader hdr{};
    hdr.magic = WalSlotHeader::MAGIC;
    hdr.version = WalSlotHeader::VERSION;
    hdr.block_layout = static_cast<std::uint8_t>(reader::BlockLayout::Gamma4K);
    hdr.slot_sequence = seq;
    hdr.slot_max_bytes = slot_size;
    hdr.db_instance_uuid = uuid;
    slot::seal_header_crc(hdr);
    EXPECT_EQ(::pwrite(fd, &hdr, sizeof(hdr), 0), static_cast<ssize_t>(sizeof(hdr)));

    std::uint64_t offset = WalSlotHeader::SIZE;
    std::uint64_t lsn = seq * 1000;
    for (const auto& records : blocks_data) {
        offset = write_gamma_block_to_fd(fd, offset, lsn++, records);
    }

    WalSlotFooter footer{};
    footer.magic = WalSlotFooter::MAGIC;
    footer.slot_sequence = seq;
    footer.sealed_write_offset = offset;
    footer.min_lsn = seq * 1000;
    footer.max_lsn = lsn - 1;
    slot::seal_footer_crc(footer);
    EXPECT_EQ(::pwrite(fd, &footer, sizeof(footer), static_cast<off_t>(slot_size - WalSlotFooter::SIZE)),
              static_cast<ssize_t>(sizeof(footer)));

    EXPECT_EQ(::fdatasync(fd), 0);
    ::close(fd);
    return offset;
}

} // namespace stratadb::wal::test
