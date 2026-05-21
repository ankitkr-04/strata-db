#include "stratadb/wal/wal_manager.hpp"

#include <atomic>
#include <cstdlib>
#include <gtest/gtest.h>
#include <string>
#include <sys/uio.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace stratadb::utils::os::test_hooks {
extern std::atomic<bool> fail_core_pinning;
extern std::atomic<bool> fail_realtime_elevation;
} // namespace stratadb::utils::os::test_hooks

namespace stratadb::io::test_hooks {
extern std::atomic<bool> mock_io_error;
extern std::atomic<bool> mock_sync_error;
extern std::atomic<ssize_t> mock_short_write_bytes;
} // namespace stratadb::io::test_hooks

namespace stratadb::wal::test {

auto create_core_test_fd() -> io::UniqueFd {
    char tpl[] = "/tmp/stratadb_core_test_XXXXXX";
    int fd = ::mkstemp(tpl);
    if (fd != -1) {
        ::unlink(tpl);
    }
    return io::UniqueFd{fd};
}

class WalManagerCoreTest : public ::testing::Test {
  protected:
    void SetUp() override {
        // Reset all external production hooks to baseline state before each run
        utils::os::test_hooks::fail_core_pinning.store(false, std::memory_order_relaxed);
        utils::os::test_hooks::fail_realtime_elevation.store(false, std::memory_order_relaxed);
        io::test_hooks::mock_io_error.store(false, std::memory_order_relaxed);
        io::test_hooks::mock_sync_error.store(false, std::memory_order_relaxed);
        io::test_hooks::mock_short_write_bytes.store(-1, std::memory_order_relaxed);
    }

    void TearDown() override {
        // Clean up hooks post-test execution to prevent cross-contamination
        utils::os::test_hooks::fail_core_pinning.store(false, std::memory_order_relaxed);
        utils::os::test_hooks::fail_realtime_elevation.store(false, std::memory_order_relaxed);
        io::test_hooks::mock_io_error.store(false, std::memory_order_relaxed);
        io::test_hooks::mock_sync_error.store(false, std::memory_order_relaxed);
        io::test_hooks::mock_short_write_bytes.store(-1, std::memory_order_relaxed);
    }
};

// 11. Core Pinning System Call Failure Recovery
TEST_F(WalManagerCoreTest, CorePinningSystemCallFailureRecovery) {
    utils::os::test_hooks::fail_core_pinning.store(true, std::memory_order_release);

    config::WalConfig cfg;
    cfg.spsc_mode = config::SpscMode::ManualOverride;
    cfg.manual_core_id = 0;

    WalManager wal(cfg, create_core_test_fd());
    wal.start_flusher();

    WriteBatch batch = {{"ping", "pong"}};
    wal.write_batch(batch);
    wal.flush();

    SUCCEED();
}

// 12. Real-Time Scheduler Elevation Failure Fallback
TEST_F(WalManagerCoreTest, RealTimeSchedulerElevationFailureFallback) {
    utils::os::test_hooks::fail_realtime_elevation.store(true, std::memory_order_release);

    config::WalConfig cfg;
    cfg.request_realtime_priority = true;

    WalManager wal(cfg, create_core_test_fd());
    wal.start_flusher();

    WriteBatch batch = {{"rt_test", "payload"}};
    wal.write_batch(batch);
    wal.flush();

    SUCCEED();
}

// 13. The Stalled Writer Trap Resolution
TEST_F(WalManagerCoreTest, StalledWriterTrapResolution) {
    config::WalConfig cfg;
    cfg.spsc_mode = config::SpscMode::Disabled;

    WalManager wal(cfg, create_core_test_fd());
    wal.start_flusher();

    WriteBatch tiny_batch = {{"k", "v"}};
    wal.write_batch(tiny_batch);

    std::atomic<bool> writer_completed{false};
    std::jthread writer_thread([&wal, &writer_completed]() {
        wal.wait_for_durable(1);
        writer_completed.store(true, std::memory_order_release);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(writer_completed.load(std::memory_order_acquire));

    wal.flush();

    writer_thread.join();
    EXPECT_TRUE(writer_completed.load(std::memory_order_acquire));
}

// 14. High-Frequency Futex Race Condition (Missed Wakeup Window)
TEST_F(WalManagerCoreTest, HighFrequencyFutexRaceCondition) {
    config::WalConfig cfg;
    WalManager wal(cfg, create_core_test_fd());

    wal.start_flusher();
    WriteBatch batch = {{"race", "check"}};
    wal.write_batch(batch);
    wal.flush();

    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // Calling wait on an already finalized and flushed sequence must bounce out instantly
    wal.wait_for_durable(1);
    SUCCEED();
}

// 15. Massive Concurrent Group Commit Squeeze Test
TEST_F(WalManagerCoreTest, MassiveConcurrentGroupCommitSqueeze) {
    config::WalConfig cfg;
    cfg.spsc_mode = config::SpscMode::Disabled;

    WalManager wal(cfg, create_core_test_fd());
    wal.start_flusher();

    constexpr int WRITERS = 64;
    std::vector<std::jthread> threads;
    threads.reserve(WRITERS);

    for (int i = 0; i < WRITERS; ++i) {
        threads.emplace_back([&wal, i]() {
            WriteBatch b = {{"thread_" + std::to_string(i), std::string(50, 'Z')}};
            wal.write_batch(b);
        });
    }

    threads.clear();
    wal.flush();

    SUCCEED();
}

// 16. Zero-Byte Empty Batch Submission
TEST_F(WalManagerCoreTest, ZeroByteEmptyBatchSubmission) {
    config::WalConfig cfg;
    WalManager wal(cfg, create_core_test_fd());
    wal.start_flusher();

    WriteBatch empty_batch;
    wal.write_batch(empty_batch);
    wal.flush();

    SUCCEED();
}

// 17. Oversized Payload Allocation Bridge
TEST_F(WalManagerCoreTest, OversizedPayloadAllocationBridge) {
    config::WalConfig cfg;
    WalManager wal(cfg, create_core_test_fd());
    wal.start_flusher();

    std::string massive_payload(40'000, 'W');
    WriteBatch large_batch = {{"giant_key", massive_payload}};

    wal.write_batch(large_batch);
    wal.flush();

    SUCCEED();
}

// 18. Critical Fault Audit: Partial / Short Write System Recovery
TEST_F(WalManagerCoreTest, CriticalFaultAuditPartialShortWriteSystemRecovery) {
    config::WalConfig cfg;
    WalManager wal(cfg, create_core_test_fd());
    wal.start_flusher();

    // Force real pwritev pipeline path inside libstratadb to encounter a short write threshold boundary
    io::test_hooks::mock_short_write_bytes.store(2048, std::memory_order_release);

    WriteBatch audit_batch = {{"fault", std::string(3000, 'F')}};
    wal.write_batch(audit_batch);
    wal.flush();

    SUCCEED();
}

// 19. Unrecoverable Hardware Disk Error & Panic Control
TEST_F(WalManagerCoreTest, UnrecoverableHardwareDiskErrorAndPanicControl) {
    config::WalConfig cfg;
    WalManager wal(cfg, create_core_test_fd());
    wal.start_flusher();

    io::test_hooks::mock_io_error.store(true, std::memory_order_release);

    WriteBatch poison_batch = {{"panic", "now"}};
    wal.write_batch(poison_batch);
    wal.flush();

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    SUCCEED();
}

// 20. Fsync Failure Handling
TEST_F(WalManagerCoreTest, FsyncFailureHandling) {
    config::WalConfig cfg;
    cfg.sync_on_commit = true;

    WalManager wal(cfg, create_core_test_fd());
    wal.start_flusher();

    io::test_hooks::mock_sync_error.store(true, std::memory_order_release);

    WriteBatch b = {{"sync_error", "test"}};
    wal.write_batch(b);
    wal.flush();

    SUCCEED();
}

// 21. Abrupt Destruction with High-Density Queue Backlog
TEST_F(WalManagerCoreTest, AbruptDestructionWithHighDensityQueueBacklog) {
    config::WalConfig cfg;
    cfg.spsc_mode = config::SpscMode::Disabled;

    auto ufd = create_core_test_fd();
    {
        WalManager wal(cfg, std::move(ufd));
        wal.start_flusher();

        for (int i = 0; i < 500; ++i) {
            WriteBatch backlog = {{"backlog_" + std::to_string(i), "data"}};
            wal.write_batch(backlog);
        }
    } // Immediate scope exit triggers abrupt RAII execution and flusher joins cleanly

    SUCCEED();
}

} // namespace stratadb::wal::test