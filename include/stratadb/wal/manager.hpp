#pragma once

#include "stratadb/config/immutable/block_pool_config.hpp"
#include "stratadb/config/immutable/wal_config.hpp"
#include "stratadb/io/posix_io_engine.hpp"
#include "stratadb/memory/block_pool.hpp"
#include "stratadb/platform/hardware_model.hpp"
#include "stratadb/platform/identity.hpp"
#include "stratadb/utils/cache.hpp"
#include "stratadb/wal/pipeline_variant.hpp"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace stratadb::config {
class ConfigManager;
}

namespace stratadb::wal::pool {
class WalSegmentPool;
}

namespace stratadb::wal {

using WriteBatch = std::vector<std::pair<std::string, std::string>>;

class WalManager {
  public:
    WalManager(const config::WalConfig& wal_cfg,
               const config::BlockPoolConfig& pool_cfg,
               const config::ConfigManager& config_mgr,
               std::filesystem::path wal_dir,
               const platform::HardwareInfo& hw_info,
               const platform::DbIdentity& db_identity);

    ~WalManager();

    WalManager(const WalManager&) = delete;
    auto operator=(const WalManager&) -> WalManager& = delete;
    WalManager(WalManager&&) = delete;
    auto operator=(WalManager&&) -> WalManager& = delete;

    void start_flusher();
    void write_batch(const WriteBatch& batch);
    void wait_for_durable(std::uint64_t target_lsn) noexcept;
    void flush() noexcept;

    [[nodiscard]] auto hw_info() const noexcept -> const platform::HardwareInfo& {
        return hw_info_;
    }
    [[nodiscard]] auto db_identity() const noexcept -> const platform::DbIdentity& {
        return db_identity_;
    }

  private:
    config::WalConfig wal_config_;
    const config::ConfigManager& config_mgr_;

    platform::HardwareInfo hw_info_;
    io::PosixIoEngine engine_;

    memory::BlockPool pool_;

    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::uint64_t> lsn_generator_{1};

    StagingVariant pipeline_;

    platform::DbIdentity db_identity_;

    std::unique_ptr<pool::WalSegmentPool> wal_segment_pool_;

    alignas(utils::CACHE_LINE_SIZE) std::atomic<std::uint64_t> durable_lsn_{0};

    std::atomic<bool> stop_requested_{false};
    std::jthread flusher_thread_{};

    void flusher_loop();

    [[nodiscard]] static auto make_pipeline(const platform::HardwareInfo::Io& io_info,
                                            const config::WalConfig& cfg,
                                            memory::BlockPool& pool,
                                            std::atomic<std::uint64_t>& lsn_gen) -> StagingVariant;
};

} // namespace stratadb::wal