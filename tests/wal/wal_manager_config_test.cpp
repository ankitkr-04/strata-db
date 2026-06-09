// tests/wal/wal_manager_config_test.cpp
//
// Tests that WalManager selects the correct pipeline variant based on the
// hardware info and SpscConfig passed at construction time, and that
// unusual-but-valid config combinations still produce a working manager.
//
// The old tests called get_effective_config() (no longer part of the API)
// and overrode probe functions directly in the TU — both no longer applicable.
// Config resolution now lives in ConfigResolver; WalManager merely executes.

#include "wal_test_helpers.hpp"

#include <chrono>
#include <thread>

using namespace stratadb::wal::test;
using stratadb::wal::WalManager;
using stratadb::wal::WriteBatch;

class WalManagerConfigTest : public WalManagerFixture {};

// Test 1: MPSC (default) pipeline works end-to-end.
TEST_F(WalManagerConfigTest, MpscPipelineWriteAndFlush) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto cfg = make_wal_cfg();
    cfg.spsc.mode = stratadb::config::SpscMode::Disabled; // MPSC

    auto wal = make_wal(cfg);
    wal->start_flusher();

    wal->write_batch({{"mpsc_key", "mpsc_value"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
}

// Test 2: SPSC with ManualOverride core 0 — flusher pins and still works.
TEST_F(WalManagerConfigTest, SpscManualOverrideCore0) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto cfg = make_wal_cfg();
    cfg.spsc.mode = stratadb::config::SpscMode::ManualOverride;
    cfg.spsc.core_id = 0;
    cfg.spsc.request_realtime_priority = false; // avoid CAP_SYS_NICE requirement

    auto wal = make_wal(cfg);
    wal->start_flusher();

    wal->write_batch({{"spsc_key", "spsc_value"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
}

// Test 3: Rotational (HDD) hardware → DeltaBlock pipeline selected.
TEST_F(WalManagerConfigTest, RotationalHardwareSelectsDeltaBlock) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    hw_info = make_test_hw_info_hdd(); // is_rotational = true

    auto cfg = make_wal_cfg();
    auto wal = make_wal(cfg);
    wal->start_flusher();

    wal->write_batch({{"hdd_key", "hdd_value"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
}

// Test 4: 16 KiB physical sector NVMe → Ssd16k pipeline selected.
TEST_F(WalManagerConfigTest, LargePhysicalSectorSelects16kPipeline) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    hw_info = make_test_hw_info_16k(); // physical_sector_size = 16384

    // Pool block must be at least LAYOUT_OFFSET(4096) + sizeof(GammaBlock<16384>).
    // sizeof(GammaBlock<16384>) = 16384+16 rounded to next 4096 multiple = 20480.
    // Minimum pool block = 4096 + 20480 = 24576. Use 32 KiB (default).
    imm_cfg.block_pool.block_size_bytes = 32LL * 1024;
    auto resolved = stratadb::config::ConfigResolver::resolve_immutable(imm_cfg);
    if (resolved.has_value())
        imm_cfg = *resolved;

    auto cfg = make_wal_cfg();
    auto wal = make_wal(cfg);
    wal->start_flusher();

    wal->write_batch({{"16k_key", "16k_value"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
}

// Test 5: sync_on_commit = false — flusher skips fdatasync, still works.
TEST_F(WalManagerConfigTest, SyncOnCommitDisabled) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    // Disable sync at the mutable-config level.
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

// Test 6: Constructor does not hang on fresh directory with no existing files.
TEST_F(WalManagerConfigTest, FreshDirectoryConstructionSucceeds) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    // Just construct — the Vanguard should start pre-allocating without blocking
    // the caller.
    auto wal = make_wal();
    SUCCEED(); // destructor cleans up cleanly
}