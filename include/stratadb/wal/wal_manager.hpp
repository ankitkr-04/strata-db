#pragma once

#include "stratadb/config/wal_config.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/epoch_manager.hpp"
#include "stratadb/wal/wal_staging.hpp"

#include <cstdint>
#include <span>
#include <variant>

namespace stratadb::wal {

using StagingVariant = std::variant<WalStaging<SectorSize::LegacyHDD>,
                                    WalStaging<SectorSize::StandardNVMe>,
                                    WalStaging<SectorSize::AdvancedFormat>,
                                    WalStaging<SectorSize::EnterpriseNVMe>

                                    >;

class WalManager {
  public:
    WalManager(const config::WalConfig& config, memory::EpochManager& epoch_manager, memory::Arena& staging_arena);

    [[nodiscard]] auto stage_write(std::uint64_t sequence_id,
                                   std::span<const std::byte> key,
                                   std::span<const std::byte> value) noexcept -> bool {

        return std::visit([&](auto& staging) -> auto { return staging.stage_write(sequence_id, key, value); },
                          staging_);
    }

    void flush_pipeline() noexcept;

  private:
    StagingVariant staging_;
    config::WalConfig config_;

    // TODO: add os-specific disk file descriptor and management
};

} // namespace stratadb::wal