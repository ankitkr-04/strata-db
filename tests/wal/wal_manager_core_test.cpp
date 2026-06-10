#include "../support/wal_test_helpers.hpp"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

using namespace stratadb::wal::test;
using stratadb::wal::WalManager;
using stratadb::wal::WriteBatch;

#ifndef NDEBUG
namespace stratadb::utils::os::test_hooks {
extern std::atomic<bool> fail_core_pinning;
extern std::atomic<bool> fail_realtime_elevation;
} // namespace stratadb::utils::os::test_hooks

namespace stratadb::io::test_hooks {
extern std::atomic<bool> mock_io_error;
extern std::atomic<bool> mock_sync_error;
extern std::atomic<ssize_t> mock_short_write_bytes;
} // namespace stratadb::io::test_hooks
#endif

class WalManagerCoreTest : public WalManagerFixture {
  protected:
    void SetUp() override {
#ifndef NDEBUG
        stratadb::utils::os::test_hooks::fail_core_pinning.store(false);
        stratadb::utils::os::test_hooks::fail_realtime_elevation.store(false);
        stratadb::io::test_hooks::mock_io_error.store(false);
        stratadb::io::test_hooks::mock_sync_error.store(false);
        stratadb::io::test_hooks::mock_short_write_bytes.store(-1);
#endif
    }

    void TearDown() override {
#ifndef NDEBUG
        stratadb::utils::os::test_hooks::fail_core_pinning.store(false);
        stratadb::utils::os::test_hooks::fail_realtime_elevation.store(false);
        stratadb::io::test_hooks::mock_io_error.store(false);
        stratadb::io::test_hooks::mock_sync_error.store(false);
        stratadb::io::test_hooks::mock_short_write_bytes.store(-1);
#endif
    }
};

TEST_F(WalManagerCoreTest, CorePinningFailureRecovery) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);
#ifdef NDEBUG
    GTEST_SKIP() << "Test hooks only available in debug builds";
#else
    stratadb::utils::os::test_hooks::fail_core_pinning.store(true);

    auto cfg = stratadb::test::test_wal_config();
    cfg.spsc.mode = stratadb::config::SpscMode::ManualOverride;
    cfg.spsc.core_id = 0;
    cfg.spsc.request_realtime_priority = false;

    auto wal = make_wal(cfg);
    wal->start_flusher();

    wal->write_batch({{"pin_fail", "v"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
#endif
}

TEST_F(WalManagerCoreTest, RealTimeElevationFailureFallback) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);
#ifdef NDEBUG
    GTEST_SKIP() << "Test hooks only available in debug builds";
#else
    stratadb::utils::os::test_hooks::fail_realtime_elevation.store(true);

    auto cfg = stratadb::test::test_wal_config();
    cfg.spsc.mode = stratadb::config::SpscMode::ManualOverride;
    cfg.spsc.core_id = 0;
    cfg.spsc.request_realtime_priority = true;

    auto wal = make_wal(cfg);
    wal->start_flusher();

    wal->write_batch({{"rt_fail", "v"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
#endif
}

TEST_F(WalManagerCoreTest, StalledWriterUnblocksAfterFlush) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto wal = make_wal();
    wal->start_flusher();

    // Inject a batch large enough to guarantee dispatch (>4 KiB).
    wal->write_batch({{"k", std::string(3500, 'Z')}});

    std::atomic<bool> done{false};
    std::jthread waiter([&] {
        wal->wait_for_durable(1);
        done.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    wal->flush();
    waiter.join();
    EXPECT_TRUE(done.load());
}

TEST_F(WalManagerCoreTest, FsyncFailureContinues) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);
#ifdef NDEBUG
    GTEST_SKIP() << "Test hooks only available in debug builds";
#else
    stratadb::io::test_hooks::mock_sync_error.store(true);

    auto wal = make_wal();
    wal->start_flusher();

    wal->write_batch({{"fsync_fail", "v"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
#endif
}

TEST_F(WalManagerCoreTest, IoErrorFlusherContinues) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);
#ifdef NDEBUG
    GTEST_SKIP() << "Test hooks only available in debug builds";
#else
    stratadb::io::test_hooks::mock_io_error.store(true);

    auto wal = make_wal();
    wal->start_flusher();

    wal->write_batch({{"io_err", "v"}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    SUCCEED();
#endif
}

TEST_F(WalManagerCoreTest, ShortWriteHandledGracefully) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);
#ifdef NDEBUG
    GTEST_SKIP() << "Test hooks only available in debug builds";
#else
    stratadb::io::test_hooks::mock_short_write_bytes.store(512);

    auto wal = make_wal();
    wal->start_flusher();

    wal->write_batch({{"short", std::string(3000, 'S')}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    SUCCEED();
#endif
}

TEST_F(WalManagerCoreTest, EmptyBatchNoWork) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto wal = make_wal();
    wal->start_flusher();

    wal->write_batch({});
    wal->flush();
    SUCCEED();
}

TEST_F(WalManagerCoreTest, MassiveConcurrentGroupCommit) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto wal = make_wal();
    wal->start_flusher();

    constexpr int kWriters = 32;
    std::vector<std::jthread> threads;
    threads.reserve(kWriters);
    for (int i = 0; i < kWriters; ++i) {
        threads.emplace_back([&wal, i] { wal->write_batch({{"t" + std::to_string(i), std::string(50, 'W')}}); });
    }
    threads.clear();
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    SUCCEED();
}

TEST_F(WalManagerCoreTest, AbruptDestructionWithBacklog) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    {
        auto wal = make_wal();
        wal->start_flusher();
        for (int i = 0; i < 300; ++i) {
            wal->write_batch({{"backlog_" + std::to_string(i), "d"}});
        }
    }
    SUCCEED();
}

TEST_F(WalManagerCoreTest, WaitForDurableAlreadySatisfied) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto wal = make_wal();
    wal->start_flusher();

    wal->write_batch({{"immediate", std::string(3000, 'I')}});
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    const auto start = std::chrono::steady_clock::now();
    wal->wait_for_durable(1);
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);

    EXPECT_LT(elapsed.count(), 500) << "wait_for_durable took too long on already-durable LSN";
}
