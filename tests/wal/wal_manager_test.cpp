#include "../support/wal_test_helpers.hpp"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

using namespace stratadb::wal::test;
using stratadb::wal::WalManager;
using stratadb::wal::WriteBatch;

namespace {

auto count_wal_files(const std::filesystem::path& dir) -> std::size_t {
    std::size_t n = 0;
    std::error_code ec;
    for (auto& e : std::filesystem::directory_iterator(dir, ec)) {
        if (e.path().extension() == ".log")
            ++n;
    }
    return n;
}

} // namespace

class WalManagerTest : public WalManagerFixture {};

TEST_F(WalManagerTest, BasicWriteAndFlush) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto wal = make_wal();
    wal->start_flusher();

    WriteBatch batch{{"hello", "world"}, {"key2", "value2"}};
    wal->write_batch(batch);
    wal->flush();

    // Give the flusher a moment to drain.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_GE(count_wal_files(wal_dir.path), 1u)
        << "Expected at least one WAL segment to be created in " << wal_dir.path;
}

TEST_F(WalManagerTest, EmptyBatch) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto wal = make_wal();
    wal->start_flusher();

    WriteBatch empty{};
    wal->write_batch(empty); // must not crash or deadlock
    wal->flush();
    // Destructor joins the flusher cleanly.
}

TEST_F(WalManagerTest, WaitForDurableSignals) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto wal = make_wal();
    wal->start_flusher();

    // Write a batch large enough that the flusher will dispatch it.
    WriteBatch batch{{"k", std::string(3000, 'X')}};
    wal->write_batch(batch);
    wal->flush();

    // wait_for_durable must not block forever once the
    // flusher has processed at least one block (LSN 1).
    std::atomic<bool> done{false};
    std::thread waiter([&] {
        wal->wait_for_durable(1);
        done.store(true, std::memory_order_release);
    });

    waiter.join();
    EXPECT_TRUE(done.load());
}

TEST_F(WalManagerTest, ConcurrentWriters) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    auto wal = make_wal();
    wal->start_flusher();

    constexpr int kThreads = 8;
    constexpr int kBatchesPerThread = 20;

    std::vector<std::jthread> writers;
    writers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        writers.emplace_back([&wal, t] {
            for (int i = 0; i < kBatchesPerThread; ++i) {
                WriteBatch b{{"thread_" + std::to_string(t), "value_" + std::to_string(i)}};
                wal->write_batch(b);
            }
        });
    }
    writers.clear(); // join all

    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    EXPECT_GE(count_wal_files(wal_dir.path), 1u);
}

TEST_F(WalManagerTest, MultipleSegmentRotation) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    // Use a very small slot so rotation happens quickly.
    auto cfg = stratadb::test::test_wal_config();
    // Minimum: WalSlotHeader(4096) + 1 block(4096) + WalSlotFooter(512) = 8704
    // Use 16 KiB so ~2-3 blocks trigger rotation.
    cfg.slot_size_bytes = 16LL * 1024;
    cfg.preallocated_pool_size = 2;

    auto wal = make_wal(cfg);
    wal->start_flusher();

    // Write many small batches; each forces at least one 4 KiB block into the
    // pipeline.  With a 16 KiB slot we expect rotation after 2-3 writes.
    for (int i = 0; i < 10; ++i) {
        WriteBatch b{{"key_" + std::to_string(i), std::string(100, 'A')}};
        wal->write_batch(b);
    }
    wal->flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // With rotation we expect > 1 segment file on disk.
    EXPECT_GE(count_wal_files(wal_dir.path), 2u) << "Expected rotation to produce multiple segment files";
}

TEST_F(WalManagerTest, CleanShutdownUnderLoad) {
    WAL_SKIP_IF_NO_ODIRECT(wal_dir.path);

    {
        auto wal = make_wal();
        wal->start_flusher();

        // Queue a large backlog before letting the destructor run.
        for (int i = 0; i < 200; ++i) {
            WriteBatch b{{"backlog_" + std::to_string(i), "v"}};
            wal->write_batch(b);
        }
        // WalManager destructor must join the flusher without deadlock.
    }
    SUCCEED();
}
