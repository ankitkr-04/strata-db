#pragma once

#include "stratadb/config/wal_config.hpp"
#include "stratadb/io/io_concept.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/epoch_manager.hpp"
#include "stratadb/wal/wal_staging.hpp"

#include <cstdint>
#include <span>
#include <stop_token>
#include <thread>
#include <variant>

namespace stratadb::wal {

using StagingVariant = std::variant<WalStaging<SectorSize::LegacyHDD>,
                                    WalStaging<SectorSize::StandardNVMe>,
                                    WalStaging<SectorSize::AdvancedFormat>,
                                    WalStaging<SectorSize::EnterpriseNVMe>

                                    >;

template <io::IsIoEngine Engine>
class WalManager {
  public:
    WalManager(const config::WalConfig& config,
               memory::EpochManager& epoch_manager,
               memory::Arena& staging_arena,
               Engine io_engine,
               int wal_fd);

    [[nodiscard]] auto stage_write(std::uint64_t sequence_id,
                                   std::span<const std::byte> key,
                                   std::span<const std::byte> value) noexcept -> bool {

        return std::visit([&](auto& staging) -> auto { return staging.stage_write(sequence_id, key, value); },
                          staging_);
    }

    void flush_pipeline() noexcept;

  private:
    void flusher_loop(std::stop_token stop_token) noexcept;

  private:
    StagingVariant staging_;
    config::WalConfig config_;

    Engine io_engine_;
    int wal_fd_{-1};

    std::jthread flusher_thread_;
};

} // namespace stratadb::wal