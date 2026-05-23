#pragma once

#include "stratadb/config/immutable/epoch_config.hpp"
#include "stratadb/utils/cache.hpp"
#include "stratadb/utils/limits.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>

namespace stratadb::memory {

enum class EpochError : std::uint8_t { ThreadLimitExceeded };

class EpochManager {
  public:
    // Default-constructed with default EpochConfig — convenient for tests and
    // call sites that don't need tuning. Production callers pass the resolved config.
    explicit EpochManager(const config::EpochConfig& cfg = config::EpochConfig{}) noexcept;

    ~EpochManager() noexcept;
    EpochManager(const EpochManager&) = delete;
    EpochManager(EpochManager&&) = delete;
    auto operator=(const EpochManager&) -> EpochManager& = delete;
    auto operator=(EpochManager&&) -> EpochManager& = delete;

    class [[nodiscard]] ReadGuard {
      public:
        [[nodiscard]] explicit ReadGuard(EpochManager& mgr) noexcept
            : mgr_(&mgr) {
            mgr_->enter();
        }
        ~ReadGuard() noexcept {
            if (mgr_)
                mgr_->leave();
        }

        ReadGuard(const ReadGuard&) = delete;
        ReadGuard(ReadGuard&&) = delete;
        auto operator=(const ReadGuard&) -> ReadGuard& = delete;
        auto operator=(ReadGuard&&) -> ReadGuard& = delete;

      private:
        EpochManager* mgr_;
    };

    class [[nodiscard]] ThreadRegistrationGuard {
      public:
        [[nodiscard]] explicit ThreadRegistrationGuard(EpochManager& mgr) noexcept
            : mgr_(mgr)
            , result_(mgr_.register_thread()) {}

        ~ThreadRegistrationGuard() noexcept {
            if (result_.has_value())
                mgr_.unregister_thread();
        }

        ThreadRegistrationGuard(const ThreadRegistrationGuard&) = delete;
        ThreadRegistrationGuard(ThreadRegistrationGuard&&) = delete;
        auto operator=(const ThreadRegistrationGuard&) -> ThreadRegistrationGuard& = delete;
        auto operator=(ThreadRegistrationGuard&&) -> ThreadRegistrationGuard& = delete;

        [[nodiscard]] auto is_registered() const noexcept -> bool {
            return result_.has_value();
        }
        [[nodiscard]] auto result() const noexcept -> std::expected<void, EpochError> {
            return result_;
        }

      private:
        EpochManager& mgr_;
        std::expected<void, EpochError> result_;
    };

    friend class ReadGuard;
    friend class ThreadRegistrationGuard;

    template <typename T>
    void retire(T* ptr) noexcept {
        if (ptr == nullptr)
            return;
        retire_node(static_cast<void*>(ptr), [](void* p) noexcept { delete static_cast<T*>(p); });
    }

    [[nodiscard]] static auto is_registered() noexcept -> bool {
        return thread_index_ != INVALID_THREAD;
    }

    [[nodiscard]] auto current_epoch() const noexcept -> std::uint64_t;

    // Internal maintenance hook: forces an epoch advance + reclaim.
    // Requires the calling thread to be registered.
    void quiescent_reclaim() noexcept;

    // Single-threaded teardown escape hatch: bypasses epoch checks, drains all retire lists.
    void force_reclaim_all() noexcept;

  private:
    static constexpr std::size_t MAX_THREADS = utils::MAX_SUPPORTED_THREADS;
    static constexpr std::size_t MASK_WORD_BITS = std::numeric_limits<std::uint64_t>::digits;
    static constexpr std::size_t ACTIVE_THREAD_MASK_WORDS = MAX_THREADS / MASK_WORD_BITS;
    static constexpr std::size_t INVALID_THREAD = std::numeric_limits<std::size_t>::max();
    static constexpr std::uint64_t INACTIVE_EPOCH = std::numeric_limits<std::uint64_t>::max();

    static_assert(MAX_THREADS % MASK_WORD_BITS == 0);

    struct RetireNode {
        void* ptr{nullptr};
        void (*deleter)(void*){nullptr};
        std::uint64_t retire_epoch{0};
    };

    struct alignas(stratadb::utils::CACHE_LINE_SIZE) ThreadState {
        std::atomic<std::uint64_t> state{INACTIVE_EPOCH};
        std::uint32_t slab_count{0};
        std::uint32_t overflow_count{0};
        std::uint32_t overflow_capacity{0};

        // 4096 bytes (one OS page / 64 cache lines) prevents TLB straddling.
        // 168 * 24 bytes = 4032 + 32 metadata + 32 padding = 4096 exactly.
        static constexpr std::size_t SLAB_CAPACITY = 168;
        RetireNode slab[SLAB_CAPACITY];

        RetireNode* overflow{nullptr};
    };

    static_assert(sizeof(ThreadState) == 4096,
                  "ThreadState must occupy exactly one OS page (4096 bytes) for TLB locality");

    struct alignas(stratadb::utils::CACHE_LINE_SIZE) ActiveThreadMaskWord {
        std::atomic<std::uint64_t> bits{0};
    };

  private:
    // ── Runtime state ─────────────────────────────────────────────────────────
    config::EpochConfig config_;
    std::size_t reclaim_interval_mask_; // config_.reclaim_interval - 1; valid because power of two

    std::atomic<std::uint64_t> global_epoch_{0};
    std::size_t instance_id_{0};

    std::array<ThreadState, MAX_THREADS> thread_states_{};
    std::array<ActiveThreadMaskWord, ACTIVE_THREAD_MASK_WORDS> active_thread_masks_{};

    inline static thread_local std::size_t thread_index_{INVALID_THREAD};

  private:
    [[nodiscard]] auto my_thread_index() const noexcept -> std::size_t;
    void retire_node(void* ptr, void (*deleter)(void*)) noexcept;
    void enter() noexcept;
    void leave() noexcept;
    void try_advance_epoch() noexcept;
    void reclaim() noexcept;
    [[nodiscard]] auto register_thread() noexcept -> std::expected<void, EpochError>;
    void unregister_thread() noexcept;
};

} // namespace stratadb::memory