#pragma once

#include "stratadb/wal/delta_block.hpp"
#include "stratadb/wal/gamma_block.hpp"
#include "stratadb/wal/wal_pipeline.hpp"

#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>
namespace stratadb::wal {

// The 3 Hardware Realities on Modern Linux:
// 1. SSD with standard 4KiB LBA
using Ssd4kPipeline = WalPipeline<GammaBlock<4096>>;
// 2. SSD with Enterprise 16KiB LBA
using Ssd16kPipeline = WalPipeline<GammaBlock<16384>>;
// 3. HDD strictly using 4096-byte physical sectors (Advanced Format 4Kn)
using Hdd4kPipeline = WalPipeline<DeltaBlock<4096>>;

using StagingVariant = std::variant<Ssd4kPipeline, Ssd16kPipeline, Hdd4kPipeline>;

// Mock WriteBatch for compilation (Replace with actual WriteBatch later)
using WriteBatch = std::vector<std::pair<std::string, std::string>>;

class WalManager {
  public:
    // The variant initializes exactly one pipeline based on sysfs detection.
    WalManager(bool is_rotational, std::size_t hw_sector_size) {
        if (is_rotational) {
            pipeline_ = Hdd4kPipeline{};
        } else if (hw_sector_size == 16384) {
            pipeline_ = Ssd16kPipeline{};
        } else {
            pipeline_ = Ssd4kPipeline{};
        }
    }

    void write_batch(const WriteBatch& batch) {

        std::visit(
            [&batch](auto& active_pipeline) -> auto {
                for (const auto& [k, v] : batch) {
                    std::span<const std::byte> key_span{reinterpret_cast<const std::byte*>(k.data()), k.size()};
                    std::span<const std::byte> val_span{reinterpret_cast<const std::byte*>(v.data()), v.size()};

                    active_pipeline.stage_write(key_span, val_span);
                }
            },
            pipeline_);
    }

  private:
    StagingVariant pipeline_;
};

} // namespace stratadb::wal