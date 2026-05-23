#pragma once

#include "stratadb/config/immutable/block_pool_config.hpp"
#include "stratadb/config/immutable/wal_config.hpp"
#include "stratadb/io/io_capabilities.hpp"
#include "stratadb/io/posix_io_engine.hpp"
#include "stratadb/io/unique_file_descriptor.hpp"
#include "stratadb/memory/block_pool.hpp"
#include "stratadb/utils/cache.hpp"
#include "stratadb/wal/pipeline_variant.hpp"

#include <atomic>
#include <cstdint>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

// Forward-declared to avoid pulling the entire ConfigManager header
// (and its EpochManager dependency) into every WAL-using translation unit.
// manager.cpp includes the full header where get_mutable() is called.
namespace stratadb::config {
class ConfigManager;
} // namespace stratadb::config

namespace stratadb::wal {

// Placeholder: will be replaced by the real WriteBatch type in the DB layer.
using WriteBatch = std::vector<std::pair<std::string, std::string>>;

// WalManager
//
// Owns the WAL I/O pipeline for a single database file descriptor.
//
// Preconditions (enforced by ConfigResolver before construction):
//   - wal_cfg.spsc.mode is Disabled or ManualOverride (AutoDiscover resolved).
//   - pool_cfg fields are valid powers-of-two.
//
// Threading model:
//   - write_batch()        : called by any number of writer threads.
//   - start_flusher()      : called once from the owning thread.
//   - flush() / wait_for_durable() : called from any thread after write_batch.
//   - All other methods    : called from the owning thread only.
//
// Lifetime:
//   - config_mgr must outlive WalManager (held by reference for WalTuning reads).
//   - Destroy WalManager before the file descriptor backing fd is closed
//     independently (WalManager owns the UniqueFd and closes it on destruction).
class WalManager {
  public:
    WalManager(const config::WalConfig& wal_cfg,
               const config::BlockPoolConfig& pool_cfg,
               const config::ConfigManager& config_mgr,
               io::UniqueFd fd);

    ~WalManager();

    WalManager(const WalManager&) = delete;
    auto operator=(const WalManager&) -> WalManager& = delete;
    WalManager(WalManager&&) = delete;
    auto operator=(WalManager&&) -> WalManager& = delete;

    // Spawns the background flusher thread.
    // Must be called exactly once after construction, before write_batch().
    void start_flusher();

    // Enqueue a batch of KV writes.  Thread-safe; any number of writers may
    // call this concurrently.
    void write_batch(const WriteBatch& batch);

    // Block until all writes with LSN <= target_lsn are durable (fdatasync'd).
    // Returns immediately if durable_lsn_ >= target_lsn.
    void wait_for_durable(std::uint64_t target_lsn) noexcept;

    // Force all partially-filled staging blocks through the pipeline.
    // Required when sync_on_commit is false and the caller needs a manual
    // durability fence.
    void flush() noexcept;

    // Exposes hardware capabilities detected at construction time.
    [[nodiscard]] auto io_capabilities() const noexcept -> const io::IOCapabilities& {
        return caps_;
    }

  private:
    config::WalConfig wal_config_;            // fully resolved by caller
    const config::ConfigManager& config_mgr_; // for reading WalTuning (mutable)

    // I/O
    io::UniqueFd fd_;
    io::IOCapabilities caps_;
    io::PosixIoEngine engine_;

    // Write-buffer pool
    // Declared BEFORE pipeline_ because WalPipeline stores a reference to it.
    memory::BlockPool pool_;

    // LSN counter
    // Declared BEFORE pipeline_ for the same reason.
    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::uint64_t> lsn_generator_{1};

    // Pipeline
    // Selected once at construction based on probed hardware; never swapped.
    StagingVariant pipeline_;

    // Durability tracking
    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::uint64_t> durable_lsn_{0};
    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::uint64_t> current_file_offset_{0};

    // Flusher thread
    // Declared LAST so its destructor (which joins the thread) runs first,
    // before pool_, engine_, fd_ and other resources the thread accesses.
    std::atomic<bool> stop_requested_{false};
    std::jthread flusher_thread_{};

    // Private helpers
    void flusher_loop();

    // Selects and constructs the correct StagingVariant based on hardware.
    // Called from the constructor initializer list once caps_ and pool_ are live.
    [[nodiscard]] static auto make_pipeline(const io::IOCapabilities& caps,
                                            const config::WalConfig& cfg,
                                            memory::BlockPool& pool,
                                            std::atomic<std::uint64_t>& lsn_gen) -> StagingVariant;
};

} // namespace stratadb::wal