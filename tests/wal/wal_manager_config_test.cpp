#include "../support/wal_test_helpers.hpp"

#include <chrono>
#include <thread>

using namespace stratadb::wal::test;
using stratadb::wal::WalManager;
using stratadb::wal::WriteBatch;

class WalManagerConfigTest : public WalManagerFixture {};

TEST_F(WalManagerConfigTest, MpscPipelineWriteAndFlush) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto cfg = stratadb::test::test_wal_config();
    cfg.spsc.mode = stratadb::config::SpscMode::Disabled;

    auto wal = make_wal(cfg);
    wal->start_flusher();

    wal->write_batch({{"mpsc_key", "mpsc_value"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
}

TEST_F(WalManagerConfigTest, SpscManualOverrideCore0) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto cfg = stratadb::test::test_wal_config();
    cfg.spsc.mode = stratadb::config::SpscMode::ManualOverride;
    cfg.spsc.core_id = 0;
    cfg.spsc.request_realtime_priority = false;

    auto wal = make_wal(cfg);
    wal->start_flusher();

    wal->write_batch({{"spsc_key", "spsc_value"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
}

TEST_F(WalManagerConfigTest, RotationalHardwareSelectsDeltaBlock) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    hw_info = make_test_hw_info_hdd();

    auto cfg = stratadb::test::test_wal_config();
    auto wal = make_wal(cfg);
    wal->start_flusher();

    wal->write_batch({{"hdd_key", "hdd_value"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
}

TEST_F(WalManagerConfigTest, LargePhysicalSectorSelects16kPipeline) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    hw_info = make_test_hw_info_16k();

    // Pool block must be at least LAYOUT_OFFSET(4096) + sizeof(GammaBlock<16384>).
    // sizeof(GammaBlock<16384>) = 16384+16 rounded to next 4096 multiple = 20480.
    // Minimum pool block = 4096 + 20480 = 24576. Use 32 KiB (default).
    imm_cfg.block_pool.block_size_bytes = 32LL * 1024;
    auto resolved = stratadb::config::ConfigResolver::resolve_immutable(imm_cfg);
    if (resolved.has_value())
        imm_cfg = *resolved;

    auto cfg = stratadb::test::test_wal_config();
    auto wal = make_wal(cfg);
    wal->start_flusher();

    wal->write_batch({{"16k_key", "16k_value"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
}

TEST_F(WalManagerConfigTest, SyncOnCommitDisabled) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto mut_cfg = stratadb::test::test_mutable_config();
    mut_cfg.wal_tuning.sync_on_commit = false;
    auto result = config_mgr.update_mutable(mut_cfg);
    ASSERT_TRUE(result.has_value());

    auto wal = make_wal();
    wal->start_flusher();

    wal->write_batch({{"async_key", "async_value"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
}

TEST_F(WalManagerConfigTest, FreshDirectoryConstructionSucceeds) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto wal = make_wal();
    SUCCEED();
}
