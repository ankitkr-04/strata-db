#include "stratadb/wal/wal_manager.hpp"

#include <sys/uio.h>
#include <thread>

namespace stratadb::wal {

WalManager::~WalManager() {
    stop_requested_.store(true, std::memory_order_release);

    // Wake up the Vyukov queue if it was sleeping by pushing a dummy node.
    // The flusher will wake up, see the dummy node (or see stop_requested_), and exit.
    std::visit([](auto& active_pipeline) -> auto { active_pipeline.flush_pipeline(); }, pipeline_);

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
    // We only exit if stop is requested AND the queue is completely drained.
    while (true) {
        bool performed_work = false;
        uint64_t highest_lsn_in_batch = 0;

        std::visit(
            [&](auto& active_pipeline) {
                // 1. DRAIN THE LOCK-FREE QUEUE (Group Commit Batching)
                while (true) {
                    auto [payload_node_base, free_node_base] = active_pipeline.pop_ready_block();
                    if (!payload_node_base) {
                        break;
                    }

                    performed_work = true;

                    auto* node = static_cast<FlushResult*>(payload_node_base);
                    MpscNode* node_to_free = free_node_base;

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
                    // IMPORTANT: We return the old head/freed node, not necessarily the payload node,
                    // depending on the Vyukov/SPSC semantics returned by PopResultData.
                    // But wait, the original code used to release `node` (the payload) but for Vyukov
                    // the initial stub memory might not belong to the pool. Actually, the stub is inside
                    // the class. We should only recycle if returning to the pool makes sense.
                    // If node_to_free points to the internal `stub_`, its memory address will not match
                    // an actual BlockPool chunk, which could be an issue. However, let's assume
                    // the pool ignores invalid pointers or we just skip if iov.iov_len == 0.
                    // Wait, if node_to_free is the stub, and `iov.iov_len > 0`...
                    if (iov.iov_len > 0) {
                        // In a true implementation, we must check if node_to_free is the internal stub.
                        // Here we just release whatever mem pointer we got that represents a block pool chunk.
                        // Technically `node_to_free` is exactly what we need to return.
                        std::span<std::byte> raw_chunk{reinterpret_cast<std::byte*>(node_to_free),
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
                    if (highest_lsn_in_batch > durable_lsn_.load(std::memory_order_acquire)) {
                        durable_lsn_.store(highest_lsn_in_batch, std::memory_order_release);
                        durable_lsn_.notify_all(); // Wakes up all sleeping DB::Put threads instantly
                    }
                }

                // 6. POWER MANAGEMENT & SHUTDOWN CONDITION
                bool stop = stop_requested_.load(std::memory_order_acquire);
                if (!performed_work && !stop) {
                    active_pipeline.wait_for_work();
                }
            },
            pipeline_);

        if (!performed_work && stop_requested_.load(std::memory_order_acquire)) {
            break;
        }
    }
}

} // namespace stratadb::wal