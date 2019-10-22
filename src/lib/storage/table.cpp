#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size)
  : _max_chunk_size{chunk_size} {
  auto first_chunk = std::make_shared<Chunk>();
  _chunks.push_back(first_chunk);
}

void Table::add_column(const std::string& name, const std::string& type) {
  _column_names.push_back(name);
  _column_types.push_back(type);
  
  // add segment of the right type to every chunk
  for(auto& chunk : _chunks) {
    auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);
    chunk->add_segment(segment);
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  // Add chunk with segments for every column if necessary
  if(_chunks.back()->size() == _max_chunk_size) {
    auto new_chunk = std::make_shared<Chunk>();;
    for(auto& type : _column_types){
      auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);
      new_chunk->add_segment(segment);
    }
    _chunks.push_back(new_chunk);
  }

  _chunks.back()->append(values);
}

uint16_t Table::column_count() const {
  return _column_names.size();
}

uint64_t Table::row_count() const {
  return (_chunks.size() - 1 ) * _max_chunk_size + _chunks.back()->size();
}

ChunkID Table::chunk_count() const {
  return ChunkID(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  // Implementation goes here
  for (size_t i = 0; i < _column_names.size(); ++i) {
    if (_column_names[i] == column_name) {
      return ColumnID(i);
    }
  }
  throw std::runtime_error("Column name not in column");
}

uint32_t Table::max_chunk_size() const {
  return _max_chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  return _column_names;
}

const std::string& Table::column_name(ColumnID column_id) const {
  DebugAssert(column_id < _column_names.size(), "Specified column_id is not in column names");
  return _column_names[column_id];
}

const std::string& Table::column_type(ColumnID column_id) const {
  DebugAssert(column_id < _column_types.size(), "Specified column_id is not in column types");
  return _column_types[column_id];
}

Chunk& Table::get_chunk(ChunkID chunk_id) { 
  DebugAssert(chunk_id < _chunks.size(), "Specified chunk_id is not in chunks");
  return *_chunks[chunk_id];
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const { 
  return get_chunk(chunk_id);
}

}  // namespace opossum
