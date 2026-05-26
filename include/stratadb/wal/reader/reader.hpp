#pragma once

#include "stratadb/wal/reader/validator.hpp"
#include "stratadb/wal/ring/ring.hpp"
#include "stratadb/wal/ring/slot_types.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <utility>
#include <vector>

namespace stratadb::wal::reader {
// A single recovered KV record yielded to the caller's callback.
//
// LIFETIME WARNING: `key` and `value` point into the WalReader's internal
// read buffer (Gamma) or scratch buffer (Delta).  Both are invalidated the
// moment the callback returns.  The MemTable (or any other consumer) must
// copy the byte ranges before returning from the callback.
struct RecoveredRecord {
    std::uint64_t lsn; // block sequence number shared by all records in the block
    std::span<const std::byte> key;
    std::span<const std::byte> value;
};

using RecordCallback = std::function<void(const RecoveredRecord)>;

enum class RecoveryStatus : std::uint8_t {
    Clean,     // all slots scanned cleanly to their valid extents
    TornWrite, // stopped at torn block; all preceding records already yielded
    IOError,   // pread failure prevented reading a slot
};

struct RecoveryResult {
    RecoveryStatus status{RecoveryStatus::Clean};
    std::uint64_t records_recovered{0};
    std::uint64_t last_valid_lsn{0};
    // Populated only when status == TornWrite:
    std::uint8_t torn_slot_index{UINT8_MAX};
    // Pass this to wal_ring.set_active_write_offset() after recovery returns.
    std::uint64_t torn_slot_valid_end{0};
};

class WalReader {
  public:
    explicit WalReader(ring::WalRing& wal_ring, BlockLayout layout);
    ~WalReader();

    WalReader(const WalReader&) = delete;
    auto operator=(const WalReader&) -> WalReader& = delete;
    WalReader(WalReader&&) = delete;
    auto operator=(WalReader&&) -> WalReader& = delete;

    [[nodiscard]] auto recover(RecordCallback callback) noexcept -> RecoveryResult;

  private:
    ring::WalRing& wal_ring_;
    BlockLayout layout_;

    // O_DIRECT-compatible read buffer.
    // 16 KiB covers GammaBlock<16384> (largest layout) and DeltaBlock<4096>.
    static constexpr std::size_t kReadBufSize = 16384;
    static constexpr std::size_t kReadBufAlign = 4096;
    void* read_buf_raw_{nullptr};     // aligned_alloc owner
    std::uint8_t* read_buf_{nullptr}; // typed alias

    // Scratch space for DeltaBlock linearization (payload bytes, CRCs stripped).
    // One full DeltaBlock<4096> of payload fits: 4096 - 4 (CRC) = 4092 bytes,
    // but we round up to a full sector for safety.
    static constexpr std::size_t kScratchSize = 4096;
    alignas(8) std::byte scratch_[kScratchSize]{};

    [[nodiscard]] auto block_size() const noexcept -> std::size_t;
    [[nodiscard]] auto header_size() const noexcept -> std::size_t;

    // Returns {status, last_good_byte_offset_in_slot}.
    // Updates prev_lsn and records_out in-place.
    [[nodiscard]] auto recover_slot(const ring::WalRing::SlotSnapshot& snap,
                                    RecordCallback& cb,
                                    std::uint64_t& prev_lsn,
                                    std::uint64_t& records_out) noexcept -> std::pair<RecoveryStatus, std::uint64_t>;
    // Return false if recovery should be aborted after this block.

    [[nodiscard]] auto parse_gamma_block(const std::uint8_t* block,
                                         std::size_t valid_end, // from GammaBlock::Header::valid_data_end_offset
                                         std::uint64_t block_seq,
                                         RecordCallback& cb,
                                         std::uint64_t& records_out) noexcept -> bool;

    [[nodiscard]] auto parse_delta_block(const std::uint8_t* block,
                                         std::size_t raw_size, // full block size; CRC validation already passed
                                         std::uint64_t block_seq,
                                         RecordCallback& cb,
                                         std::uint64_t& records_out) noexcept -> bool;

    // Copies Delta payload bytes (skipping the 16-byte block header and every
    // per-sector 4-byte CRC slot) into scratch_[].
    // Returns the number of logical payload bytes written.
    [[nodiscard]] auto linearize_delta(const std::uint8_t* raw_block, std::size_t raw_size) noexcept -> std::size_t;
};

} // namespace stratadb::wal::reader
