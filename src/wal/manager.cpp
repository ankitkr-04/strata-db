#include "stratadb/wal/manager.hpp"

#include "stratadb/config/config_manager.hpp"       // ConfigManager::get_mutable()
#include "stratadb/config/immutable/wal_config.hpp" // SpscMode
#include "stratadb/utils/os.hpp"
#include "stratadb/utils/probe.hpp"

#include <sys/uio.h>

namespace stratadb::wal {

// make_pipeline
//
// Pure factory: maps (hardware capabilities, resolved WAL config) to the
// correct StagingVariant alternative.  Called exactly once from the
// constructor initializer list after caps_ and pool_ are live.
//
// Selection logic:
//   - Rotational media   → DeltaBlock (per-sector CRC, no RMW on spinning disk)
//   - 16 KiB physical sectors → GammaBlock<16384>
//   - Default (NVMe 4 KiB)   → GammaBlock<4096>
//   - SPSC (ManualOverride)  → SpscMailboxQueue (busy-polling flusher)
//   - Otherwise              → VyukovMpscQueue  (futex-sleeping flusher)
auto WalManager::make_pipeline(const io::IOCapabilities& caps,
                               const config::WalConfig& cfg,
                               memory::BlockPool& pool,
                               std::atomic<std::uint64_t>& lsn_gen) -> StagingVariant {
    const bool use_spsc = (cfg.spsc.mode == config::SpscMode::ManualOverride);

    if (caps.is_rotational) {
        return use_spsc ? StagingVariant{std::in_place_type<Hdd4kSpscPipeline>, pool, lsn_gen}
                        : StagingVariant{std::in_place_type<Hdd4kMpscPipeline>, pool, lsn_gen};
    }
    if (caps.physical_sector_size == 16384) {
        return use_spsc ? StagingVariant{std::in_place_type<Ssd16kSpscPipeline>, pool, lsn_gen}
                        : StagingVariant{std::in_place_type<Ssd16kMpscPipeline>, pool, lsn_gen};
    }
    // Default: 4 KiB NVMe / SATA SSD
    return use_spsc ? StagingVariant{std::in_place_type<Ssd4kSpscPipeline>, pool, lsn_gen}
                    : StagingVariant{std::in_place_type<Ssd4kMpscPipeline>, pool, lsn_gen};
}


// Initializer list order MUST match the declaration order in manager.hpp:
//   wal_config_ → config_mgr_ → fd_ → caps_ → engine_ → pool_
//   → lsn_generator_ → pipeline_ → ...
//
// pool_ and lsn_generator_ are initialized before pipeline_ so that
// WalPipeline's stored references are valid when make_pipeline() runs.
WalManager::WalManager(const config::WalConfig& wal_cfg,
                       const config::BlockPoolConfig& pool_cfg,
                       const config::ConfigManager& config_mgr,
                       io::UniqueFd fd)
    : wal_config_(wal_cfg)
    , config_mgr_(config_mgr)
    , fd_(std::move(fd))
    , caps_(utils::probe_io_capabilities(fd_.get()))
    , engine_(caps_)
    , pool_(pool_cfg)
    , lsn_generator_{1}
    , pipeline_(make_pipeline(caps_, wal_config_, pool_, lsn_generator_)) {}


// Order of events:
//   1. Signal the flusher to stop.
//   2. Seal any remaining staged blocks and wake the flusher.
//   3. flusher_thread_ destructor joins (because it is declared last in the
//      class and therefore destroyed first — before pool_, engine_, fd_).
WalManager::~WalManager() {
    stop_requested_.store(true, std::memory_order_release);
    // shutdown() seals remaining blocks and calls force_wakeup() — do NOT call
    // flush_pipeline() here as that pushes a sentinel that may already be
    // at the queue head, creating a self-loop.
    std::visit([](auto& p) { p.shutdown(); }, pipeline_);
    // flusher_thread_ is destroyed (and joined) automatically after this body.
}

void WalManager::start_flusher() {
    flusher_thread_ = std::jthread([this] { flusher_loop(); });
}

// write_batch
//
// std::visit resolves the variant once per batch (not per record).
// Inside the visitor the compiler sees the concrete WalPipeline type and
// inlines stage_write() — no virtual dispatch on the per-record path.
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

// Runs on the dedicated flusher thread.  The loop:
//   1. Applies OS-level thread physics (core pinning, RT priority).
//   2. Drains the lock-free queue in a group-commit batch.
//   3. Issues a single pwritev per drained block.
//   4. Reads sync_on_commit from the live MutableConfig via ConfigManager
//      (a single atomic load + epoch pin — negligible overhead per batch).
//   5. If sync requested, calls fdatasync once for the entire batch.
//   6. Advances durable_lsn_ and wakes waiting writers.
//   7. Sleeps (futex or cpu_relax) when idle; exits when stop_requested_
//      is true AND the queue is fully drained.
void WalManager::flusher_loop() {
    // ── 1. Thread physics ─────────────────────────────────────────────────────
    if (wal_config_.spsc.mode == config::SpscMode::ManualOverride) {
        if (!utils::os::pin_current_thread_to_core(wal_config_.spsc.core_id.value())) {
            // Log: core pinning failed; continuing without pinning.
        }
    }
    if (wal_config_.request_realtime_priority) {
        if (!utils::os::elevate_to_realtime_priority()) {
            // Log: RT elevation failed; continuing at normal priority.
        }
    }

    // ── 2. Hot loop ───────────────────────────────────────────────────────────
    while (true) {
        bool performed_work = false;
        uint64_t highest_lsn_in_batch = 0;

        std::visit(
            [&](auto& active_pipeline) {
                // ── Drain the queue (group-commit batching) ───────────────────
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

                    // ── pwritev ───────────────────────────────────────────────
                    // Sentinels carry an empty span; skip the write but still
                    // recycle their memory if needed.
                    if (!node->memory_to_write.empty()) {
                        struct iovec iov{
                            // POSIX iov_base is void* (non-const); kernel won't modify.
                            .iov_base = const_cast<void*>(static_cast<const void*>(node->memory_to_write.data())),
                            .iov_len = node->memory_to_write.size(),
                        };
                        const std::uint64_t file_offset = current_file_offset_.load(std::memory_order_relaxed);

                        auto result = engine_.writev(fd_.get(), std::span(&iov, 1), file_offset);

                        if (result.has_value()) {
                            current_file_offset_.fetch_add(iov.iov_len, std::memory_order_relaxed);
                        } else {
                            // Fatal I/O error — log and mark, but keep draining
                            // so the flusher can still release pool blocks.
                        }
                    }

                    // ── Pool recycle ──────────────────────────────────────────
                    // node_to_free may differ from payload_node in Vyukov MPSC
                    // (it's the old dummy head, not the current payload).
                    if (node_to_free && node_to_free->pool_managed) {
                        std::span<std::byte> raw{reinterpret_cast<std::byte*>(node_to_free), pool_.block_size()};
                        pool_.release_block(raw);
                    }
                }

                // ── fdatasync + ACID acknowledgment ───────────────────────────
                if (performed_work) {
                    // Read sync_on_commit from the live mutable config.
                    // ReadGuard pins the epoch for the duration of this expression;
                    // the bool is copied out before the guard is released.
                    const bool should_sync = config_mgr_.get_mutable()->wal_tuning.sync_on_commit;

                    if (should_sync) {
                        auto sync_result = engine_.sync(fd_.get());
                        if (!sync_result) {
                            // Fatal sync failure — database is in an
                            // unrecoverable state.  Log and abort.
                        }
                    }

                    // Advance durable_lsn_ and wake all waiting writers.
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

                // ── Power management ──────────────────────────────────────────
                if (!performed_work && !stop_requested_.load(std::memory_order_acquire)) {
                    active_pipeline.wait_for_work(stop_requested_);
                }
            },
            pipeline_);

        // Exit only when stop is signalled AND the queue is fully drained.
        if (!performed_work && stop_requested_.load(std::memory_order_acquire)) {
            break;
        }
    }
}

} // namespace stratadb::wal