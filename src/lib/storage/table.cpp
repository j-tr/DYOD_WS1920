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

Table::Table(const uint32_t chunk_size){
  _max_chunk_size = chunk_size;
  auto first_chunk = std::make_shared<Chunk>();
  _chunks.push_back(first_chunk);
}

void Table::add_column(const std::string& name, const std::string& type) {
  // Implementation goes here
  _column_names.push_back(name);
  _column_types.push_back(type);
  
  // add segment of the right type to every chunk
  for(auto& chunk : _chunks) {
    auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);
    chunk->add_segment(segment);
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  // Implementation goes here

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
  // Implementation goes here
  return _column_names.size();
}

uint64_t Table::row_count() const {
  // Implementation goes here
  return (_chunks.size() - 1 ) * _max_chunk_size + _chunks.back()->size();
}

ChunkID Table::chunk_count() const {
  // Implementation goes here
  return ChunkID(_chunks.size());
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  // Implementation goes here
  ColumnID column_id = ColumnID(std::distance(_column_names.begin(), std::find(_column_names.begin(), _column_names.end(), column_name)));
  if(column_id >= ColumnID(_column_names.size())){
    throw std::exception();
  }
  return column_id;
}

uint32_t Table::max_chunk_size() const {
  // Implementation goes here
  return _max_chunk_size;
}

const std::vector<std::string>& Table::column_names() const {
  //throw std::runtime_error("Implement Table::column_names()");
  return _column_names;
}

const std::string& Table::column_name(ColumnID column_id) const {
  //throw std::runtime_error("Implement Table::column_name");
  return _column_names[column_id];
}

const std::string& Table::column_type(ColumnID column_id) const {
  //throw std::runtime_error("Implement Table::column_type");
  return _column_types[column_id];
}

Chunk& Table::get_chunk(ChunkID chunk_id) { 
  //throw std::runtime_error("Implement Table::get_chunk"); 
  return *_chunks[chunk_id];
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const { 
  //throw std::runtime_error("Implement Table::get_chunk"); }
  return *_chunks[chunk_id];
}

}  // namespace opossum
