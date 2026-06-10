#pragma once

#include "../helper/test_config_helper.hpp"
#include "stratadb/memory/arena.hpp"
#include "stratadb/memory/tlab.hpp"
#include "stratadb/memtable/skiplist_memtable.hpp"

#include <gtest/gtest.h>

namespace stratadb::test {

struct MemTableFixture : ::testing::Test {
    stratadb::memory::Arena arena{stratadb::memory::Arena::create(stratadb::helper::make_test_memory_config()).value()};
    stratadb::memory::TLAB tlab{arena};
    stratadb::memtable::SkipListMemTable mt{arena, stratadb::helper::make_test_memtable_config()};
};

[[nodiscard]] inline auto make_test_arena(std::size_t capacity = 0, std::size_t block_size = 0) {
    if (capacity == 0) {
        return stratadb::memory::Arena::create(stratadb::helper::make_test_memory_config()).value();
    }
    if (block_size == 0) {
        return stratadb::memory::Arena::create(stratadb::helper::make_test_memory_config(capacity)).value();
    }
    return stratadb::memory::Arena::create(stratadb::helper::make_test_memory_config(capacity, block_size)).value();
}

[[nodiscard]] inline auto
make_test_memtable(stratadb::memory::Arena& arena,
                   stratadb::config::MemTableConfig cfg = stratadb::helper::make_test_memtable_config())
    -> stratadb::memtable::SkipListMemTable {
    return stratadb::memtable::SkipListMemTable{arena, cfg};
}

} // namespace stratadb::test
