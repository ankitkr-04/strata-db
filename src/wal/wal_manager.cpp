#include "stratadb/wal/wal_manager.hpp"

#include <sys/uio.h>
#include <thread>

namespace stratadb::wal {

WalManager::~WalManager() {
    stop_requested_.store(true, std::memory_order_release);

    // Wake up the Vyukov queue if it was sleeping by pushing a dummy node.
    // The flusher will wake up, see the dummy node (or see stop_requested_), and exit.
    std::visit(
        [](auto& active_pipeline) -> auto {
            active_pipeline.flush_pipeline();
        },
        pipeline_);

    // jthread automatically joins here
}

void WalManager::start_flusher() {
    // Simply spawn the thread. The thread itself must do the OS pinning.
    flusher_thread_ = std::jthread([this] { flusher_loop(); });
}

void WalManager::flusher_loop() {
    // 1. APPLY OS-LEVEL THREAD PHYSICS INSIDE THE TARGET THREAD
    if (effective_config_.spsc_mode == config::SpscMode::ManualOverride) {
        auto succeeded = utils::os::pin_current_thread_to_core(effective_config_.manual_core_id.value());
        if (!succeeded) {
            // Log: Warning, pinning failed.
        }
    }
    if (effective_config_.request_realtime_priority) {
        auto succeeded = utils::os::elevate_to_realtime_priority();
        if (!succeeded) {
            // Log: Warning, RT elevation failed.
        }
    }

    // 2. THE HOT LOOP
    while (!stop_requested_.load(std::memory_order_acquire)) {
        bool performed_work = false;
        uint64_t highest_lsn_in_batch = 0;

        std::visit(
            [&](auto& active_pipeline) {
                // 1. DRAIN THE LOCK-FREE QUEUE (Group Commit Batching)
                while (FlushResult* node = active_pipeline.pop_ready_block()) {
                    performed_work = true;

                    // Track the highest LSN we are about to write
                    if (node->max_lsn > highest_lsn_in_batch) {
                        highest_lsn_in_batch = node->max_lsn;
                    }

                    // 2. CONSTRUCT POSIX SCATTER-GATHER ARRAY
                    struct iovec iov;
                    iov.iov_base = const_cast<std::byte*>(node->memory_to_write.data());
                    iov.iov_len = node->memory_to_write.size();

                    const uint64_t physical_offset = current_file_offset_.load(std::memory_order_relaxed);

                    // 3. EXECUTE I/O ENGINE WRITE
                    // Note: If memory_to_write is empty, this was our shutdown dummy node. Skip write.
                    if (iov.iov_len > 0) {
                        auto result = engine_.writev(fd_.get(), std::span(&iov, 1), physical_offset);
                        if (result.has_value()) {
                            current_file_offset_.fetch_add(iov.iov_len, std::memory_order_relaxed);
                        } else {
                            // Fatal Hardware Error: Log and gracefully abort
                        }
                    }

                    // 4. MEMORY RECYCLING
                    if (iov.iov_len > 0) {
                        std::span<std::byte> raw_chunk{reinterpret_cast<std::byte*>(node),
                                                       memory::BlockPool::BLOCK_SIZE};
                        pool_.release_block(raw_chunk);
                    }
                }

                // 5. DURABILITY SYNC & ACID ACKNOWLEDGMENT
                if (performed_work) {
                    if (effective_config_.sync_on_commit) {
                        auto sync_result = engine_.sync(fd_.get());
                        // If sync fails, the database is in a fatal state.
                        if (!sync_result) {
                            // Log: Sync failed, database is in an unrecoverable state. Abort.
                        }
                    }

                    // WAKE UP WAITING WRITERS!
                    // This is the C++20 standard way to do Group Commit notifications.
                    if (highest_lsn_in_batch > 0) {
                        durable_lsn_.store(highest_lsn_in_batch, std::memory_order_release);
                        durable_lsn_.notify_all(); // Wakes up all sleeping DB::Put threads instantly
                    }
                }

                // 6. POWER MANAGEMENT (If queue was empty)
                if (!performed_work && !stop_requested_.load(std::memory_order_acquire)) {
                    active_pipeline.wait_for_work();
                }
            },
            pipeline_);
    }
}

} // namespace stratadb::wal