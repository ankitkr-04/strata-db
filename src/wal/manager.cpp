#include "stratadb/wal/manager.hpp"

#include "stratadb/config/config_manager.hpp"       // ConfigManager::get_mutable()
#include "stratadb/config/immutable/wal_config.hpp" // SpscMode
#include "stratadb/utils/os.hpp"

#include <sys/uio.h>

namespace stratadb::wal {

// Selection logic:
//   - Rotational media        → DeltaBlock (per-sector CRC, no RMW on spinning disk)
//   - 16 KiB physical sectors → GammaBlock<16384>
//   - Default (NVMe 4 KiB)    → GammaBlock<4096>
//   - SPSC (ManualOverride)   → SpscMailboxQueue (busy-polling flusher)
//   - Otherwise               → VyukovMpscQueue  (futex-sleeping flusher)
auto WalManager::make_pipeline(const platform::HardwareInfo::Io& io_info,
                               const config::WalConfig& cfg,
                               memory::BlockPool& pool,
                               std::atomic<std::uint64_t>& lsn_gen) -> StagingVariant {
    const bool use_spsc = (cfg.spsc.mode == config::SpscMode::ManualOverride);

    if (io_info.is_rotational) {
        return use_spsc ? StagingVariant{std::in_place_type<Hdd4kSpscPipeline>, pool, lsn_gen}
                        : StagingVariant{std::in_place_type<Hdd4kMpscPipeline>, pool, lsn_gen};
    }
    if (io_info.physical_sector_size == 16384) {
        return use_spsc ? StagingVariant{std::in_place_type<Ssd16kSpscPipeline>, pool, lsn_gen}
                        : StagingVariant{std::in_place_type<Ssd16kMpscPipeline>, pool, lsn_gen};
    }
    // Default: NVMe/SATA SSD with 4 KiB or 512B sectors (we pad to 4K safely)
    return use_spsc ? StagingVariant{std::in_place_type<Ssd4kSpscPipeline>, pool, lsn_gen}
                    : StagingVariant{std::in_place_type<Ssd4kMpscPipeline>, pool, lsn_gen};
}

// Initializer list order MUST match the declaration order in manager.hpp:
//   wal_config_ → config_mgr_ → fd_ → hw_info_ → engine_ → pool_
//   → lsn_generator_ → pipeline_ → ...
WalManager::WalManager(const config::WalConfig& wal_cfg,
                       const config::BlockPoolConfig& pool_cfg,
                       const config::ConfigManager& config_mgr,
                       io::UniqueFd fd,
                       const platform::HardwareInfo& hw_info,
                       const platform::DbIdentity& db_identity)
    : wal_config_(wal_cfg)
    , config_mgr_(config_mgr)
    , fd_(std::move(fd))
    , hw_info_(hw_info)    // 1. Store the global hardware truth
    , engine_(hw_info_.io) // 2. Pass the IO slice to the engine
    , pool_(pool_cfg)
    , lsn_generator_{1}
    , pipeline_(make_pipeline(hw_info_.io, wal_config_, pool_, lsn_generator_)) // 3. Build the pipeline
    , db_identity_(db_identity) {}

WalManager::~WalManager() {
    stop_requested_.store(true, std::memory_order_release);
    std::visit([](auto& p) { p.shutdown(); }, pipeline_);
    // flusher_thread_ joins automatically.
}

void WalManager::start_flusher() {
    flusher_thread_ = std::jthread([this] { flusher_loop(); });
}

void WalManager::write_batch(const WriteBatch& batch) {
    std::visit(
        [&batch](auto& pipeline) {
            for (const auto& [k, v] : batch) {
                pipeline.stage_write(std::span<const std::byte>(reinterpret_cast<const std::byte*>(k.data()), k.size()),
                                     std::span<const std::byte>(reinterpret_cast<const std::byte*>(v.data()),
                                                                v.size()));
            }
        },
        pipeline_);
}

void WalManager::wait_for_durable(std::uint64_t target_lsn) noexcept {
    std::uint64_t cur = durable_lsn_.load(std::memory_order_acquire);
    while (cur < target_lsn) {
        durable_lsn_.wait(cur, std::memory_order_acquire);
        cur = durable_lsn_.load(std::memory_order_acquire);
    }
}

void WalManager::flush() noexcept {
    std::visit([](auto& p) { p.flush_pipeline(); }, pipeline_);
}

void WalManager::flusher_loop() {
    if (wal_config_.spsc.mode == config::SpscMode::ManualOverride) {
        // We can now safely validate the core_id against our hardware info
        auto core_id = wal_config_.spsc.core_id.value();
        if (core_id < hw_info_.cpu.logical_count) {
            if (!utils::os::pin_current_thread_to_core(core_id)) {
                // Log: core pinning failed
            }
        }

        if (wal_config_.spsc.request_realtime_priority) {
            if (!utils::os::elevate_to_realtime_priority()) {
                // Log: RT elevation failed
            }
        }
    }

    while (true) {
        bool performed_work = false;
        uint64_t highest_lsn_in_batch = 0;

        std::visit(
            [&](auto& active_pipeline) {
                while (true) {
                    auto [payload_base, free_base] = active_pipeline.pop_ready_block();
                    if (!payload_base) {
                        break;
                    }

                    performed_work = true;

                    auto* node = static_cast<FlushResult*>(payload_base);
                    auto* node_to_free = free_base;

                    if (node->max_lsn > highest_lsn_in_batch) {
                        highest_lsn_in_batch = node->max_lsn;
                    }

                    if (!node->memory_to_write.empty()) {
                        struct iovec iov{
                            .iov_base = const_cast<void*>(static_cast<const void*>(node->memory_to_write.data())),
                            .iov_len = node->memory_to_write.size(),
                        };

                        // FUTURE RING BUFFER INJECTION POINT:
                        // Here is where we will check if (current_file_offset_ + iov.iov_len > slot_size)
                        // and trigger the WalSlot sealing (writing Footer) and ring buffer advance.
                        const std::uint64_t file_offset = current_file_offset_.load(std::memory_order_relaxed);

                        auto result = engine_.writev(fd_.get(), std::span(&iov, 1), file_offset);

                        if (result.has_value()) {
                            current_file_offset_.fetch_add(iov.iov_len, std::memory_order_relaxed);
                        } else {
                            // Fatal I/O error
                        }
                    }

                    if (node_to_free && node_to_free->pool_managed) {
                        std::span<std::byte> raw{reinterpret_cast<std::byte*>(node_to_free), pool_.block_size()};
                        pool_.release_block(raw);
                    }
                }

                if (performed_work) {
                    const bool should_sync = config_mgr_.get_mutable()->wal_tuning.sync_on_commit;

                    if (should_sync) {
                        auto sync_result = engine_.sync(fd_.get());
                        if (!sync_result) {
                            // Fatal sync failure
                        }
                    }

                    if (highest_lsn_in_batch > 0) {
                        std::uint64_t cur = durable_lsn_.load(std::memory_order_relaxed);
                        while (highest_lsn_in_batch > cur
                               && !durable_lsn_.compare_exchange_weak(cur,
                                                                      highest_lsn_in_batch,
                                                                      std::memory_order_release,
                                                                      std::memory_order_relaxed)) {
                        }
                        durable_lsn_.notify_all();
                    }
                }

                if (!performed_work && !stop_requested_.load(std::memory_order_acquire)) {
                    active_pipeline.wait_for_work(stop_requested_);
                }
            },
            pipeline_);

        if (!performed_work && stop_requested_.load(std::memory_order_acquire)) {
            break;
        }
    }
}

} // namespace stratadb::wal