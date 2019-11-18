#include "reference_segment.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table> referenced_table, const ColumnID referenced_column_id, const std::shared_ptr<const PosList> pos)
  : _referenced_table(referenced_table),
    _referenced_column_id(referenced_column_id),
    _pos_list(pos) {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  Assert(chunk_offset <= size(), "Operator []: The referenced value does not exist");
  auto& chunk = referenced_table()->get_chunk(pos_list()->at(chunk_offset).chunk_id);
  auto column = chunk.get_segment(referenced_column_id());
  return (*column)[pos_list()->at(chunk_offset).chunk_offset];
}

  const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const {
    return _pos_list;
  }
  const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const {
    return _referenced_table;
  }

  ColumnID ReferenceSegment::referenced_column_id() const {
    return _referenced_column_id;
  }


size_t ReferenceSegment::size() const {
  return pos_list()->size();
}

size_t ReferenceSegment::estimate_memory_usage() const {
  return 0;
}

}
