#pragma once

#include "stratadb/wal/block/delta_block.hpp"
#include "stratadb/wal/block/gamma_block.hpp"
#include "stratadb/wal/concepts.hpp"
#include "stratadb/wal/pipeline.hpp"
#include "stratadb/wal/queue/spsc_mailbox_queue.hpp"
#include "stratadb/wal/queue/vyukov_mpsc_queue.hpp"

#include <variant>

namespace stratadb::wal {

// Verify queue implementations satisfy the concept at the assembly point,
// not scattered across individual queue headers.
static_assert(ConcurrencyQueue<VyukovMpscQueue>, "VyukovMpscQueue does not satisfy ConcurrencyQueue");
static_assert(ConcurrencyQueue<SpscMailboxQueue>, "SpscMailboxQueue does not satisfy ConcurrencyQueue");

// Naming scheme: <Device><SectorKiB>k<QueueType>Pipeline
//
//   Ssd  = NVMe / SATA SSD (non-rotational, AWUPF atomicity guarantee)
//   Hdd  = spinning disk (rotational, per-sector CRC essential)
//   4k   = 4 KiB GammaBlock or DeltaBlock sector granularity
//   16k  = 16 KiB GammaBlock (enterprise NVMe with 16 KiB physical sectors)
//   Mpsc = multi-producer / single-consumer Vyukov queue
//   Spsc = per-thread SPSC mailbox ring buffers (pinned-core mode)

using Ssd4kMpscPipeline = WalPipeline<GammaBlock<4096>, VyukovMpscQueue>;
using Ssd4kSpscPipeline = WalPipeline<GammaBlock<4096>, SpscMailboxQueue>;
using Ssd16kMpscPipeline = WalPipeline<GammaBlock<16384>, VyukovMpscQueue>;
using Ssd16kSpscPipeline = WalPipeline<GammaBlock<16384>, SpscMailboxQueue>;
using Hdd4kMpscPipeline = WalPipeline<DeltaBlock<4096>, VyukovMpscQueue>;
using Hdd4kSpscPipeline = WalPipeline<DeltaBlock<4096>, SpscMailboxQueue>;

// Holds exactly one live pipeline, selected once during WalManager construction
// based on hardware capabilities reported by probe_io_capabilities().
// std::visit resolves the active alternative once per write_batch — no dynamic
// dispatch on the per-record hot path inside the visitor.
using StagingVariant = std::variant<Ssd4kMpscPipeline,
                                    Ssd4kSpscPipeline,
                                    Ssd16kMpscPipeline,
                                    Ssd16kSpscPipeline,
                                    Hdd4kMpscPipeline,
                                    Hdd4kSpscPipeline>;

} // namespace stratadb::wal