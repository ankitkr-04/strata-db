#pragma once

#include "stratadb/memory/tlab.hpp"

#include <concepts>
#include <optional>
#include <string_view>

namespace stratadb::memtable {

template <typename T>
concept IsMemTable =
    requires(T t, std::string_view key, std::string_view value, memory::TLAB& tlab, std::size_t flush_trigger_bytes) {
        // Inserts a key-value pair. Returns true on success (e.g., not OOM).
        { t.put(key, value, tlab, flush_trigger_bytes) } -> std::same_as<bool>;

        // Inserts a tombstone for a key. Returns true on success.
        { t.remove(key, tlab) } -> std::same_as<bool>;

        // Gets a value. Returns a zero-copy view into the Arena, or nullopt.
        { t.get(key) } -> std::same_as<std::optional<std::string_view>>;

        // Exposes current memory footprint for flush triggers.
        { t.memory_usage() } -> std::same_as<std::size_t>;

        // tryye when memory_usage() exceeds flush_trigger_bytes, false otherwise.
        { t.should_flush(flush_trigger_bytes) } -> std::same_as<bool>;
    };

} // namespace stratadb::memtable