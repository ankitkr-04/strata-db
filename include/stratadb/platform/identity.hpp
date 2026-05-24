#pragma once

#include <array>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <string>

namespace stratadb::platform {
struct DbIdentity {
    std::array<uint8_t, 16> bytes{};

    [[nodiscard]] auto to_string() const noexcept -> std::string;
};

enum class DBIdentityError : std::uint8_t {
    IOError, // Failed to read/write the identity file

};

// Reads <data_dir>/IDENTITY and returns its UUID.
// On first run (file absent) or a corrupted file (partial write survived a crash),
// generates a fresh UUID v4, writes the file with full fsync durability, and returns it.
[[nodiscard]] auto load_or_create_identity(const std::filesystem::path& data_dir) noexcept
    -> std::expected<DbIdentity, DBIdentityError>;

} // namespace stratadb::platform
