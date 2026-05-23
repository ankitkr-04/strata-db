#pragma once

#include "stratadb/config/immutable_config.hpp"
#include "stratadb/config/mutable_config.hpp"
#include "stratadb/memory/epoch_manager.hpp"
#include "stratadb/utils/cache.hpp"

#include <atomic>
#include <expected>
#include <mutex>

namespace stratadb::config {

enum class ConfigError : std::uint8_t {
    OutOfMemory,
};

class ConfigManager {
  public:
    // noexcept by design: OOM during construction is unrecoverable; terminates.
    ConfigManager(ImmutableConfig imm, MutableConfig mut, memory::EpochManager& epoch_mgr) noexcept;

    ~ConfigManager() noexcept;

    ConfigManager(const ConfigManager&) = delete;
    auto operator=(const ConfigManager&) -> ConfigManager& = delete;
    ConfigManager(ConfigManager&&) = delete;
    auto operator=(ConfigManager&&) -> ConfigManager& = delete;

    // RAII guard that pins an EpochManager read epoch for its lifetime.
    // Non-copyable, non-movable: relies on guaranteed copy elision (C++17).
    class [[nodiscard]] ReadGuard {
      public:
        ReadGuard(const ReadGuard&) = delete;
        auto operator=(const ReadGuard&) -> ReadGuard& = delete;
        ReadGuard(ReadGuard&&) = delete;
        auto operator=(ReadGuard&&) -> ReadGuard& = delete;

        ~ReadGuard() = default;

        [[nodiscard]] auto operator->() const noexcept -> const MutableConfig* {
            return config_;
        }
        [[nodiscard]] auto get() const noexcept -> const MutableConfig& {
            return *config_;
        }

      private:
        friend class ConfigManager;

        [[nodiscard]] explicit ReadGuard(const ConfigManager& manager) noexcept
            : epoch_guard_(manager.epoch_mgr_)
            , config_(manager.current_mutable_config_.load(std::memory_order_acquire)) {}

        memory::EpochManager::ReadGuard epoch_guard_;
        const MutableConfig* config_;
    };

    [[nodiscard]] auto get_mutable() const noexcept -> ReadGuard;

    auto update_mutable(MutableConfig new_cfg) -> std::expected<void, ConfigError>;

  private:
    ImmutableConfig immutable_config_;

    // Padded to its own cache line to avoid false sharing with update_mutex_
    // and epoch_mgr_ on the write path.
    alignas(stratadb::utils::CACHE_LINE_SIZE) std::atomic<MutableConfig*> current_mutable_config_{nullptr};

    memory::EpochManager& epoch_mgr_;
    std::mutex update_mutex_;
};

} // namespace stratadb::config