#include "stratadb/wal/manager.hpp"

#include "stratadb/config/config_manager.hpp"
#include "stratadb/config/immutable/wal_config.hpp"
#include "stratadb/utils/os.hpp"
#include "stratadb/wal/pool/segment_pool.hpp"
#include "stratadb/wal/reader/validator.hpp"

#include <sys/uio.h>

namespace stratadb::wal {

namespace {

[[nodiscard]] auto determine_layout(const platform::HardwareInfo::Io& io) noexcept -> reader::BlockLayout {
    if (io.is_rotational) {
        return reader::BlockLayout::Delta4K;
    }
    if (io.physical_sector_size == 16384) {
        return reader::BlockLayout::Gamma16K;
    }
    return reader::BlockLayout::Gamma4K;
}

} // namespace

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
    return use_spsc ? StagingVariant{std::in_place_type<Ssd4kSpscPipeline>, pool, lsn_gen}
                    : StagingVariant{std::in_place_type<Ssd4kMpscPipeline>, pool, lsn_gen};
}

WalManager::WalManager(const config::WalConfig& wal_cfg,
                       const config::BlockPoolConfig& pool_cfg,
                       const config::ConfigManager& config_mgr,
                       std::filesystem::path wal_dir,
                       const platform::HardwareInfo& hw_info,
                       const platform::DbIdentity& db_identity)
    : wal_config_(wal_cfg)
    , config_mgr_(config_mgr)
    , hw_info_(hw_info)
    , engine_(hw_info_.io)
    , pool_(pool_cfg)
    , lsn_generator_{1}
    , pipeline_(make_pipeline(hw_info_.io, wal_config_, pool_, lsn_generator_))
    , db_identity_(db_identity)
    , wal_segment_pool_(std::make_unique<pool::WalSegmentPool>(std::move(wal_dir),
                                                               determine_layout(hw_info_.io),
                                                               wal_cfg.slot_size_bytes,
                                                               static_cast<uint8_t>(wal_cfg.preallocated_pool_size + 2),
                                                               wal_cfg.preallocated_pool_size,
                                                               db_identity_.bytes)) {}

WalManager::~WalManager() {
    stop_requested_.store(true, std::memory_order_release);
    std::visit([](auto& p) { p.shutdown(); }, pipeline_);
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
        const auto core_id = wal_config_.spsc.core_id.value();
        if (core_id < hw_info_.cpu.logical_count) {
            if (!utils::os::pin_current_thread_to_core(core_id)) {
            }
        }
        if (wal_config_.spsc.request_realtime_priority) {
            if (!utils::os::elevate_to_realtime_priority()) {
            }
        }
    }

    if (wal_segment_pool_->recovery_segment_index() != UINT8_MAX) {
        wal_segment_pool_->seal_active_segment_only();
    }
    wal_segment_pool_->ensure_active_segment();

    while (true) {
        bool performed_work = false;
        uint64_t highest_lsn_in_batch = 0;

        std::visit(
            [&](auto& active_pipeline) {
                const auto tuning_guard = config_mgr_.get_mutable();
                const bool should_sync = tuning_guard->wal_tuning.sync_on_commit;
                const float trigger_ratio = tuning_guard->wal_tuning.precreate_trigger_ratio;

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
                        const size_t block_size = node->memory_to_write.size();

                        if (wal_segment_pool_->needs_rotation(block_size)) {
                            wal_segment_pool_->seal_and_rotate();
                        }

                        struct iovec iov{
                            .iov_base = const_cast<void*>(static_cast<const void*>(node->memory_to_write.data())),
                            .iov_len = block_size,
                        };

                        auto result = engine_.writev(wal_segment_pool_->active_fd(),
                                                     std::span(&iov, 1),
                                                     wal_segment_pool_->active_write_offset());

                        if (result.has_value()) {
                            wal_segment_pool_->advance_write_offset(iov.iov_len);

                            wal_segment_pool_->record_block_lsn(node->max_lsn);

                            if (wal_segment_pool_->active_fill_ratio() >= trigger_ratio) {
                                wal_segment_pool_->notify_precreate();
                            }
                        } else {
                        }
                    }

                    if (node_to_free && node_to_free->pool_managed) {
                        std::span<std::byte> raw{reinterpret_cast<std::byte*>(node_to_free), pool_.block_size()};
                        pool_.release_block(raw);
                    }
                }

                if (performed_work) {
                    if (should_sync) {
                        if (!engine_.sync(wal_segment_pool_->active_fd())) {
                        }
                    }

                    if (highest_lsn_in_batch > 0) {
                        uint64_t cur = durable_lsn_.load(std::memory_order_relaxed);
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
            wal_segment_pool_->seal_active_segment_only();
            break;
        }
    }
}

} // namespace stratadb::wal