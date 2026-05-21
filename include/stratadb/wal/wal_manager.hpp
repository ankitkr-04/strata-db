#pragma once

#include "stratadb/config/wal_config.hpp"
#include "stratadb/io/posix_io_engine.hpp"
#include "stratadb/io/unique_file_descriptor.hpp"
#include "stratadb/utils/hardware.hpp"
#include "stratadb/utils/os.hpp"
#include "stratadb/wal/delta_block.hpp"
#include "stratadb/wal/gamma_block.hpp"
#include "stratadb/wal/spsc_mailbox_queue.hpp"
#include "stratadb/wal/vyukov_mpsc_queue.hpp"
#include "stratadb/wal/wal_concept.hpp"
#include "stratadb/wal/wal_pipeline.hpp"

#include <span>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace stratadb::wal {

static_assert(ConcurrencyQueue<SpscMailboxQueue>);
static_assert(ConcurrencyQueue<VyukovMpscQueue>);

// --- 1. SSD 4K Standard ---
using Ssd4kMpscPipeline = WalPipeline<GammaBlock<4096>, VyukovMpscQueue>;
using Ssd4kSpscPipeline = WalPipeline<GammaBlock<4096>, SpscMailboxQueue>;

// --- 2. SSD 16K Enterprise ---
using Ssd16kMpscPipeline = WalPipeline<GammaBlock<16384>, VyukovMpscQueue>;
using Ssd16kSpscPipeline = WalPipeline<GammaBlock<16384>, SpscMailboxQueue>;

// --- 3. HDD 4K Padded ---
using Hdd4kMpscPipeline = WalPipeline<DeltaBlock<4096>, VyukovMpscQueue>;
using Hdd4kSpscPipeline = WalPipeline<DeltaBlock<4096>, SpscMailboxQueue>;

// The Variant that holds exactly one of these realities at runtime
using StagingVariant = std::variant<Ssd4kMpscPipeline,
                                    Ssd4kSpscPipeline,
                                    Ssd16kMpscPipeline,
                                    Ssd16kSpscPipeline,
                                    Hdd4kMpscPipeline,
                                    Hdd4kSpscPipeline>;

// Mock WriteBatch
using WriteBatch = std::vector<std::pair<std::string, std::string>>;

class WalManager {
  public:
    WalManager(const config::WalConfig& requested_config, io::UniqueFd fd)
        : requested_config_(requested_config)
        , effective_config_(requested_config)
        , // Copy initially to act as the baseline
        fd_(std::move(fd))
        ,
        // 1. Probe the hardware using the FD
        caps_(utils::probe_io_capabilities(fd_.get()))
        ,
        // 2. Init the IO Engine with the exact physical capabilities
        engine_(caps_)
        , pipeline_(std::in_place_type<Ssd4kMpscPipeline>,
                    pool_,
                    lsn_generator_) // Temporary default, will be replaced in the constructor body
    {
        // 3. Compute Effective Config (Degrading safely if OS lacks requirements)
        const bool use_spsc = compute_effective_config();

        // 4. Instantiate the correct lock-free template pipeline
        if (caps_.is_rotational) {
            if (use_spsc)
                pipeline_.emplace<Hdd4kSpscPipeline>(pool_, lsn_generator_);
            else
                pipeline_.emplace<Hdd4kMpscPipeline>(pool_, lsn_generator_);
        } else if (caps_.physical_sector_size == 16384) {
            if (use_spsc)
                pipeline_.emplace<Ssd16kSpscPipeline>(pool_, lsn_generator_);
            else
                pipeline_.emplace<Ssd16kMpscPipeline>(pool_, lsn_generator_);
        } else {
            if (use_spsc)
                pipeline_.emplace<Ssd4kSpscPipeline>(pool_, lsn_generator_);
            else
                pipeline_.emplace<Ssd4kMpscPipeline>(pool_, lsn_generator_);
        }
    }

    ~WalManager();

    // Disable copying and moving: resource ownership (fd, thread) is unique.
    WalManager(const WalManager&) = delete;
    auto operator=(const WalManager&) -> WalManager& = delete;
    WalManager(WalManager&&) = delete;
    auto operator=(WalManager&&) -> WalManager& = delete;

    // Allows the system or user to query what architecture actually loaded.
    [[nodiscard]] auto get_effective_config() const noexcept -> const config::WalConfig& {
        return effective_config_;
    }

    void write_batch(const WriteBatch& batch) {
        // std::visit resolves the variant ONCE per batch.
        std::visit(
            [&batch](auto& active_pipeline) -> auto {
                // We are now INSIDE the strongly typed template.
                // This loop runs with pure inline assembly. No virtual calls.
                for (const auto& [k, v] : batch) {
                    std::span<const std::byte> key_span{reinterpret_cast<const std::byte*>(k.data()), k.size()};
                    std::span<const std::byte> val_span{reinterpret_cast<const std::byte*>(v.data()), v.size()};

                    active_pipeline.stage_write(key_span, val_span);
                }
            },
            pipeline_);
    }
    // Called by user threads after write_batch() if they need synchronous ACID guarantees
    void wait_for_durable(uint64_t target_lsn) noexcept {
        uint64_t current = durable_lsn_.load(std::memory_order_acquire);
        while (current < target_lsn) {
            // C++20 futex sleep: sleep until durable_lsn_ changes from 'current'
            durable_lsn_.wait(current, std::memory_order_acquire);
            current = durable_lsn_.load(std::memory_order_acquire);
        }
    }

    void start_flusher();

    // Forces all partially filled blocks to the Flusher (Solves the Stalled Writer Trap)
    void flush() noexcept {
        std::visit([](auto& active_pipeline) -> auto { active_pipeline.flush_pipeline(); }, pipeline_);
    }

  private:
    config::WalConfig requested_config_;
    config::WalConfig effective_config_; // Represents runtime reality

    io::UniqueFd fd_;
    io::IOCapabilities caps_;
    io::PosixIoEngine engine_;
    StagingVariant pipeline_;
    std::atomic<uint64_t> durable_lsn_{
        0}; // The highest LSN that has been durably flushed. Updated by the Flusher thread after each successful write.
    memory::BlockPool pool_;
    alignas(utils::CACHE_LINE_SIZE) std::atomic<uint64_t> lsn_generator_{1};
    std::atomic<uint64_t> current_file_offset_{0};

    // Background Thread State
    std::atomic<bool> stop_requested_{false};
    std::jthread flusher_thread_{};

    // Declare the flusher loop function
    void flusher_loop();

    // Evaluates constraints, updates effective_config_, and returns true if SPSC is safe.
    [[nodiscard]] auto compute_effective_config() noexcept -> bool {
        const uint32_t total_cores = utils::logical_core_count();

        // 1. Hard constraint: System too small for SPSC (need at least 3 cores so OS doesn't die)
        if (total_cores <= 2) {
            effective_config_.spsc_mode = config::SpscMode::Disabled;
            effective_config_.manual_core_id = std::nullopt;
            return false;
        }

        // 2. State Machine Routing
        switch (effective_config_.spsc_mode) {
            case config::SpscMode::Disabled:
                return false;

            case config::SpscMode::ManualOverride: {
                if (!effective_config_.manual_core_id.has_value()
                    || effective_config_.manual_core_id.value() >= total_cores) {
                    // Invalid manual core requested. Degrade safely to MPSC.
                    effective_config_.spsc_mode = config::SpscMode::Disabled;
                    effective_config_.manual_core_id = std::nullopt;
                    return false;
                }
                return true; // Use the exact core the power-user requested
            }

            case config::SpscMode::AutoDiscover: {
                auto isolated_core = utils::os::auto_discover_isolated_core();
                if (isolated_core.has_value()) {
                    // Success! Lock it in as a manual override so the user can see what we picked.
                    effective_config_.spsc_mode = config::SpscMode::ManualOverride;
                    effective_config_.manual_core_id = isolated_core;
                    return true;
                } else {
                    // No isolated cores found. Degrade safely to MPSC.
                    effective_config_.spsc_mode = config::SpscMode::Disabled;
                    effective_config_.manual_core_id = std::nullopt;
                    return false;
                }
            }
        }

        return false;
    }
};

} // namespace stratadb::wal