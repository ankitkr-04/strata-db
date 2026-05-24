#include "stratadb/platform/identity.hpp"

#include "stratadb/utils/os.hpp"

#include <cstring>
#include <fcntl.h>
#include <optional>
#include <random>
#include <string_view>
#include <unistd.h>

namespace stratadb::platform {
static auto generate_uuid_v4() noexcept -> DbIdentity {
    DbIdentity id{};

    std::random_device rd;
    static_assert(sizeof(unsigned int) == 4);

    for (std::size_t i = 0; i < 16; i += sizeof(unsigned int)) {
        unsigned int val = rd();
        std::memcpy(id.bytes.data() + i, &val, sizeof(unsigned int));
    }

    id.bytes[6] = static_cast<std::uint8_t>((id.bytes[6] & 0x0FU) | 0x40U); // version 4
    id.bytes[8] = static_cast<std::uint8_t>((id.bytes[8] & 0x3FU) | 0x80U); // variant 10xx

    return id;
}

auto DbIdentity::to_string() const noexcept -> std::string {
    static constexpr char kHex[] = "0123456789abcdef";

    std::string s;
    s.reserve(36);

    static constexpr std::size_t kDashAfter[] = {3, 5, 7, 9}; // byte indices after which to insert '-'

    for (std::size_t i = 0; i < 16; ++i) {
        s += kHex[bytes[i] >> 4];
        s += kHex[bytes[i] & 0x0FU];

        for (auto d : kDashAfter) {
            if (i == d) {
                s += '-';
                break;
            }
        }
    }

    return s;
}

static auto parse_uuid(std::string_view s) noexcept -> std::optional<DbIdentity> {
    // Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars)
    if (s.size() != 36) {
        return std::nullopt;
    }

    static constexpr std::size_t kDashAt[] = {8, 13, 18, 23};

    auto hex_val = [](char c) -> int {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'a' && c <= 'f')
            return c - 'a' + 10;
        if (c >= 'A' && c <= 'F')
            return c - 'A' + 10;
        return -1;
    };

    DbIdentity id{};
    std::size_t byte_idx = 0;

    for (std::size_t i = 0; i < 36;) {
        bool at_dash = false;
        for (std::size_t d : kDashAt) {
            if (i == d) {
                at_dash = true;
                break;
            }
        }
        if (at_dash) {
            if (s[i] != '-')
                return std::nullopt;
            ++i;
            continue;
        }

        const int hi = hex_val(s[i]);
        const int lo = hex_val(s[i + 1]);
        if (hi < 0 || lo < 0)
            return std::nullopt;

        id.bytes[byte_idx++] = static_cast<std::uint8_t>((hi << 4) | lo);
        i += 2;
    }

    if ((id.bytes[6] >> 4) != 4)
        return std::nullopt; // version must be 4
    if ((id.bytes[8] >> 6) != 2)
        return std::nullopt; // variant must be 10xx

    return id;
}

// Writes the IDENTITY file atomically with full crash safety:
//   1. Write content to IDENTITY.tmp
//   2. fsync the file fd  → data reaches disk before rename
//   3. rename .tmp → IDENTITY  (atomic on POSIX within the same filesystem)
//   4. fsync the directory fd  → the directory entry update (rename) reaches disk
//
// Without step 2: power loss after rename leaves a zero-byte or partial file.
// Without step 4: power loss after rename may leave the old directory entry on reboot.
static auto write_identity_atomic(const std::filesystem::path& identity_path, const DbIdentity& id) noexcept -> bool {
    const auto tmp_path = std::filesystem::path{identity_path}.replace_extension(".tmp");

    const std::string content = "# StrataDB instance identity. Do not modify or delete.\n" + id.to_string() + '\n';
    const int fd = ::open(tmp_path.c_str(), // NOLINT(cppcoreguidelines-pro-type-vararg)
                          O_WRONLY | O_CREAT | O_TRUNC,
                          0644);
    if (fd < 0)
        return false;

    const auto* data = content.data();
    auto remaining = content.size();

    while (remaining > 0) {
        const ssize_t n = ::write(fd, data, remaining);
        if (n <= 0) {
            ::close(fd);
            ::unlink(tmp_path.c_str());
            return false;
        }
        data += n;
        remaining -= static_cast<std::size_t>(n);
    }
    if (!utils::os::sync_data(fd)) {
        ::close(fd);
        ::unlink(tmp_path.c_str());
        return false;
    }
    ::close(fd);
    // Rename is atomic on POSIX; on Windows it is best-effort.
    if (::rename(tmp_path.c_str(), identity_path.c_str()) != 0) {
        ::unlink(tmp_path.c_str());
        return false;
    }

    // Best-effort: if this fails the rename is visible in the journal and will
    // survive all but the most pathological power-loss scenarios.
    const int dir_fd = ::open(identity_path.parent_path().c_str(), // NOLINT
                              O_RDONLY | O_DIRECTORY);
    if (dir_fd >= 0) {
        ::fsync(dir_fd);
        ::close(dir_fd);
    }

    return true;
}

static auto try_read_identity(const std::filesystem::path& path) noexcept -> std::optional<DbIdentity> {
    const int fd = ::open(path.c_str(), O_RDONLY); // NOLINT
    if (fd < 0)
        return std::nullopt;

    char buf[128]{};
    const ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
    ::close(fd);

    if (n <= 0)
        return std::nullopt;

    // Scan line by line; skip comment lines.
    std::string_view content{buf, static_cast<std::size_t>(n)};
    std::size_t pos = 0;

    while (pos < content.size()) {
        const std::size_t end = content.find('\n', pos);
        const std::size_t line_end = (end == std::string_view::npos) ? content.size() : end;

        std::string_view line = content.substr(pos, line_end - pos);
        pos = (end == std::string_view::npos) ? content.size() : end + 1;

        // Trim trailing CR
        if (!line.empty() && line.back() == '\r')
            line.remove_suffix(1);

        if (!line.empty() && line[0] != '#') {
            return parse_uuid(line);
        }
    }

    return std::nullopt;
}

auto load_or_create_identity(const std::filesystem::path& data_dir) noexcept
    -> std::expected<DbIdentity, DBIdentityError> {

    const auto identity_path = data_dir / "IDENTITY";

    // try_read_identity returns nullopt for missing, empty, or unparseable files —
    // all treated identically: generate a new UUID and overwrite.
    std::error_code ec;
    if (std::filesystem::exists(identity_path, ec) && !ec) {
        auto maybe_id = try_read_identity(identity_path);
        if (maybe_id.has_value())
            return *maybe_id;
        // Fall through: file corrupt, recreate below.
    }
    // Generate a new UUID and write it with full fsync durability.
    const DbIdentity id = generate_uuid_v4();
    if (!write_identity_atomic(identity_path, id)) {
        return std::unexpected(DBIdentityError::IOError);
    }

    return id;
}

} // namespace stratadb::platform