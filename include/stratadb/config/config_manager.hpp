
#pragma once

#include "immutable_config.hpp"
#include "mutable_config.hpp"
#include "stratadb/memory/epoch_manager.hpp"

#include <atomic>

namespace stratadb::config {

class ConfigManager {
  public:
    ConfigManager(ImmutableConfig imm, MutableConfig mut, memory::EpochManager& epoch_mgr) noexcept;

    ~ConfigManager() noexcept;

    ConfigManager(const ConfigManager&) = delete;
    ConfigManager& operator=(const ConfigManager&) = delete;

    ConfigManager(ConfigManager&&) = delete;
    ConfigManager& operator=(ConfigManager&&) = delete;

    class ReadGuard {
      public:
        ReadGuard(memory::EpochManager* mgr, const MutableConfig* cfg) noexcept
            : epoch_mgr_(mgr)
            , config_(cfg) {}

        ~ReadGuard() noexcept {
            if (epoch_mgr_ != nullptr) {
                epoch_mgr_->leave();
            }
        }

        ReadGuard(const ReadGuard&) = delete;
        ReadGuard& operator=(const ReadGuard&) = delete;

        ReadGuard(ReadGuard&& other) noexcept
            : epoch_mgr_(other.epoch_mgr_)
            , config_(other.config_) {
            other.epoch_mgr_ = nullptr;
            other.config_ = nullptr;
        }

        ReadGuard& operator=(ReadGuard&& other) noexcept {
            if (this != &other) {

                if (epoch_mgr_ != nullptr) {
                    epoch_mgr_->leave();
                }
                epoch_mgr_ = other.epoch_mgr_;
                config_ = other.config_;
                other.epoch_mgr_ = nullptr;
                other.config_ = nullptr;
            }
            return *this;
        }

        [[nodiscard]]
        const MutableConfig* operator->() const noexcept {
            return config_;
        }

        [[nodiscard]]
        const MutableConfig& get() const noexcept {
            return *config_;
        }

      private:
        memory::EpochManager* epoch_mgr_;
        const MutableConfig* config_;
    };

    [[nodiscard]]
    ReadGuard get_mutable() const noexcept;

    [[nodiscard]]
    const ImmutableConfig& get_immutable() const noexcept {
        return immutable_;
    }

    void update_mutable(MutableConfig new_cfg) noexcept;

  private:
    ImmutableConfig immutable_;

    std::atomic<MutableConfig*> current_mutable_{nullptr};

    memory::EpochManager& epoch_mgr_;
};

} // namespace stratadb::config