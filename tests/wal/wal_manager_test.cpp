#include "stratadb/wal/wal_manager.hpp"

#include <cstdlib>
#include <gtest/gtest.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace stratadb::wal::test {

auto create_temp_file() -> io::UniqueFd {
    char tpl[] = "/tmp/stratadb_wal_test_XXXXXX";
    int fd = mkstemp(tpl);
    if (fd != -1) {
        unlink(tpl); // Unlink so it cleans up when closed
    }
    return io::UniqueFd{fd};
}

TEST(WalManagerTest, SpscDowngradeOnSmallSystemsOrInvalidCores) {
    config::WalConfig cfg;
    // 1. Invalid manual override
    cfg.spsc_mode = config::SpscMode::ManualOverride;
    cfg.manual_core_id = 999999;

    WalManager wal1(cfg, create_temp_file());
    EXPECT_EQ(wal1.get_effective_config().spsc_mode, config::SpscMode::Disabled);
    EXPECT_FALSE(wal1.get_effective_config().manual_core_id.has_value());

    // NOTE: AutoDiscover might fail on i3 with 2 cores as logical_core_count <= 2
    // If it fails, it also downgrades to Disabled.
}

TEST(WalManagerTest, BasicWriteBatch) {
    config::WalConfig cfg;
    cfg.spsc_mode = config::SpscMode::Disabled;
    cfg.sync_on_commit = true;

    auto ufd = create_temp_file();
    int fd = ufd.get();
    WalManager wal(cfg, std::move(ufd));

    wal.start_flusher();

    WriteBatch batch;
    std::string large_value(5000, 'X');
    batch.emplace_back("hello", large_value);
    batch.emplace_back("key2", "value2");

    wal.write_batch(batch);

    wal.flush();

    // Batch written. wait_for_durable on LSN 1.
    // LSN calculation depends on total updates. Mpsc pipeline increments it.
    // Since we forced an allocation and a dispatch (5000 bytes > 4096 bytes block size),
    // at least LSN 1 will be durably synced.
    wal.wait_for_durable(1);

    off_t size = lseek(fd, 0, SEEK_END);
    EXPECT_GT(size, 0); // Something must have been written
}

TEST(WalManagerTest, EmptyBatch) {
    config::WalConfig cfg;
    cfg.spsc_mode = config::SpscMode::Disabled;

    auto ufd = create_temp_file();
    WalManager wal(cfg, std::move(ufd));
    wal.start_flusher();

    WriteBatch batch; // Empty
    wal.write_batch(batch);

    // Since nothing is written, wait_for_durable wouldn't know when to return if it expected LSN updates,
    // so we just let the destructor naturally clean up the WAL, asserting it handles empty batches gracefully.
    // The flusher should just shut down cleanly.
}

TEST(WalManagerTest, ConcurrentGroupCommit) {
    config::WalConfig cfg;
    cfg.spsc_mode = config::SpscMode::Disabled; // Force MPSC

    auto ufd = create_temp_file();
    int fd = ufd.get();
    WalManager wal(cfg, std::move(ufd));
    wal.start_flusher();

    constexpr int NUM_THREADS = 10;
    std::vector<std::jthread> threads;

    // We'll write to the WAL from multiple threads concurrently.
    threads.reserve(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&wal, i]() -> void {
            WriteBatch batch;
            std::string large_value(5000, 'Y');
            batch.emplace_back("thread" + std::to_string(i), large_value);
            wal.write_batch(batch);
        });
    }

    threads.clear(); // Waits for all pushing threads to complete

    wal.flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // We expect some positive LSN. We can't know the exact LSN each thread got without
    // WalManager returning it, so we simply verify that it stops cleanly.

    // To ensure durablity, we just ensure file has size > 0
    off_t size = lseek(fd, 0, SEEK_END);
    EXPECT_GT(size, 0);
}

} // namespace stratadb::wal::test