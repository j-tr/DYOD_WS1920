#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/resolve_type.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/storage/dictionary_segment.hpp"

namespace opossum {

class StorageTableTest : public BaseTest {
 protected:
  void SetUp() override {
    t.add_column("col_1", "int");
    t.add_column("col_2", "string");
  }

  Table t{2};
};

TEST_F(StorageTableTest, ChunkCount) {
  EXPECT_EQ(t.chunk_count(), 1u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.chunk_count(), 2u);
}

TEST_F(StorageTableTest, GetChunk) {
  t.get_chunk(ChunkID(ChunkID{0}));
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.get_chunk(ChunkID(ChunkID{q}), std::exception);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  t.get_chunk(ChunkID(ChunkID{1}));
}

TEST_F(StorageTableTest, ColumnCount) { EXPECT_EQ(t.column_count(), 2u); }

TEST_F(StorageTableTest, RowCount) {
  EXPECT_EQ(t.row_count(), 0u);
  t.append({4, "Hello,"});
  t.append({6, "world"});
  t.append({3, "!"});
  EXPECT_EQ(t.row_count(), 3u);
}

TEST_F(StorageTableTest, GetColumnName) {
  EXPECT_EQ(t.column_name(ColumnID{0}), "col_1");
  EXPECT_EQ(t.column_name(ColumnID{1}), "col_2");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.column_name(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnType) {
  EXPECT_EQ(t.column_type(ColumnID{0}), "int");
  EXPECT_EQ(t.column_type(ColumnID{1}), "string");
  // TODO(anyone): Do we want checks here?
  // EXPECT_THROW(t.column_type(ColumnID{2}), std::exception);
}

TEST_F(StorageTableTest, GetColumnIdByName) {
  EXPECT_EQ(t.column_id_by_name("col_2"), 1u);
  EXPECT_THROW(t.column_id_by_name("no_column_name"), std::exception);
}

TEST_F(StorageTableTest, GetChunkSize) { EXPECT_EQ(t.max_chunk_size(), 2u); }

TEST_F(StorageTableTest, CompressChunk) {
  t.append({4, "Hello,"});
  t.append({1, "Hello,"});
  t.append({6, "world"});
  t.append({6, "A"});

  t.compress_chunk(ChunkID(0));
  t.compress_chunk(ChunkID(1));

  auto segment_0_0 = std::dynamic_pointer_cast<DictionarySegment<int>>((t.get_chunk(ChunkID(0)).get_segment(ColumnID(0))));
  auto segment_0_1 = std::dynamic_pointer_cast<DictionarySegment<std::string>>((t.get_chunk(ChunkID(0)).get_segment(ColumnID(1))));
  auto segment_1_0 = std::dynamic_pointer_cast<DictionarySegment<int>>((t.get_chunk(ChunkID(1)).get_segment(ColumnID(0))));
  auto segment_1_1 = std::dynamic_pointer_cast<DictionarySegment<std::string>>((t.get_chunk(ChunkID(1)).get_segment(ColumnID(1))));

  // Test dictionary size (uniqueness)
  EXPECT_EQ(segment_0_0->unique_values_count(), 2u);
  EXPECT_EQ(segment_0_1->unique_values_count(), 1u);
  EXPECT_EQ(segment_1_0->unique_values_count(), 1u);
  EXPECT_EQ(segment_1_1->unique_values_count(), 2u);

  // Test attribute_vector size
  EXPECT_EQ(segment_0_0->size(), 2u);
  EXPECT_EQ(segment_0_1->size(), 2u);
  EXPECT_EQ(segment_1_0->size(), 2u);
  EXPECT_EQ(segment_1_1->size(), 2u);

  // Test sorting
  EXPECT_EQ((*(segment_0_0->dictionary()))[0], 1);
  EXPECT_EQ((*(segment_0_0->dictionary()))[1], 4);
  EXPECT_EQ((*(segment_1_1->dictionary()))[0], "A");
  EXPECT_EQ((*(segment_1_1->dictionary()))[1], "world");
 }

}  // namespace opossum
