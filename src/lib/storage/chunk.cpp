#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) {
  // Implementation goes here
  _segments.push_back(segment);
}

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  // Implementation goes here
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const {
  // Implementation goes here
  return _segments[column_id];
}

uint16_t Chunk::column_count() const {
  // Implementation goes here
  return _segments.size();
}

uint32_t Chunk::size() const {
  // Implementation goes here
  if (_segments.size() != 0) {
  	return _segments[0]->size();
  } else {
  	return 0;
  }
}

}  // namespace opossum
