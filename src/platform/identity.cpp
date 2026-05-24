#include "stratadb/platform/identity.hpp"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <random>

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

static auto write_identity_atomic(const std::filesystem::path& identity_path, const DbIdentity& id) noexcept -> bool {
    const auto tmp_path = std::filesystem::path{identity_path}.replace_extension(".tmp");

    {
        std::ofstream out(tmp_path, std::ios::out | std::ios::trunc);
        if (!out)
            return false;

        out << "# StrataDB instance identity. Do not modify or delete.\n" << id.to_string() << '\n';

        if (!out)
            return false;
        out.flush();
        if (!out)
            return false;
    }

    // Rename is atomic on POSIX; on Windows it is best-effort.
    std::error_code ec;
    std::filesystem::rename(tmp_path, identity_path, ec);
    if (ec) {
        std::filesystem::remove(tmp_path, ec);
        return false;
    }

    return true;
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

auto load_or_create_identity(const std::filesystem::path& data_dir) noexcept
    -> std::expected<DbIdentity, DBIdentityError> {

    const auto identity_path = data_dir / "IDENTITY";

    std::error_code ec;
    if (std::filesystem::exists(identity_path, ec) && !ec) {
        std::ifstream in(identity_path);
        if (!in)
            return std::unexpected(DBIdentityError::IOError);

        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty() && line[0] != '#') {
                // Trim trailing whitespace / CR
                while (!line.empty() && (line.back() == '\r' || line.back() == '\n' || line.back() == ' ')) {
                    line.pop_back();
                }
                auto result = parse_uuid(line);
                if (!result)
                    return std::unexpected(DBIdentityError::ParseError);
                return *result;
            }
        }
        return std::unexpected(DBIdentityError::ParseError);
    }

    const DbIdentity id = generate_uuid_v4();

    if (!write_identity_atomic(identity_path, id)) {
        return std::unexpected(DBIdentityError::IOError);
    }

    return id;
}

} // namespace stratadb::platform