#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dictionary_segment.hpp"
#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _max_chunk_size{chunk_size} {
  auto first_chunk = std::make_shared<Chunk>();
  _chunks.push_back(first_chunk);
}

void Table::add_column(const std::string& name, const std::string& type) {
  Assert(row_count() == 0, "Cannot add column to non-emtpy table");
  _column_names.push_back(name);
  _column_types.push_back(type);

  // add segment of the right type to every chunk
  for (auto& chunk : _chunks) {
    auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);
    chunk->add_segment(segment);
  }
}

void Table::append(std::vector<AllTypeVariant> values) {
  // Add chunk with segments for every column if necessary
  if (_chunks.back()->size() == _max_chunk_size) {
    auto new_chunk = std::make_shared<Chunk>();
    for (auto& type : _column_types) {
      auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);
      new_chunk->add_segment(segment);
    }
    _chunks.push_back(new_chunk);
  }
  _chunks.back()->append(values);
}

uint16_t Table::column_count() const { return _column_names.size(); }

uint64_t Table::row_count() const {
  uint64_t row_count = 0;
  for (auto& chunk : _chunks) {
    row_count += chunk->size();
  }
  return row_count;
}

ChunkID Table::chunk_count() const { return ChunkID(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  // Implementation goes here
  for (ColumnID column_id(0); column_id < _column_names.size(); ++column_id) {
    if (_column_names[column_id] == column_name) {
      return column_id;
    }
  }
  throw std::runtime_error("Column name not in column");
}

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const {
  DebugAssert(column_id < _column_names.size(), "No table with given ID");
  return _column_names[column_id];
}

const std::string& Table::column_type(ColumnID column_id) const {
  DebugAssert(column_id < _column_types.size(), "No table with given ID");
  return _column_types[column_id];
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "No chunk with given ID");
  std::shared_lock read_lock(_chunk_access);
  return *_chunks[chunk_id];
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  DebugAssert(chunk_id < _chunks.size(), "No chunk with given ID");
  std::shared_lock read_lock(_chunk_access);
  return *_chunks[chunk_id];
}

void Table::compress_chunk(ChunkID chunk_id) {
  auto& chunk = get_chunk(chunk_id);

  // create structures and lambda function
  std::vector<std::thread> threads;
  std::vector<std::shared_ptr<BaseSegment>> compressed_segments(chunk.column_count());
  auto compress = [&compressed_segments](auto type, auto segment, auto segment_id) {
    compressed_segments.at(segment_id) = make_shared_by_data_type<BaseSegment, DictionarySegment>(type, segment);
  };

  // start thread for each segment
  for (size_t column_index = 0; column_index < chunk.size(); ++column_index) {
    std::shared_ptr<BaseSegment> segment = chunk.get_segment(ColumnID(column_index));
    std::string type = column_type(ColumnID(column_index));
    threads.push_back(std::thread(compress, type, segment, column_index));
  }

  // join threads and add segment to chunk
  Chunk dictionary_chunk;
  for (size_t thread_id = 0; thread_id < threads.size(); ++thread_id) {
    threads[thread_id].join();
    dictionary_chunk.add_segment(compressed_segments.at(thread_id));
  }

  // replace uncompressed chunk by compressed chunk
  std::unique_lock write_lock(_chunk_access);
  chunk = std::move(dictionary_chunk);
}

}  // namespace opossum
