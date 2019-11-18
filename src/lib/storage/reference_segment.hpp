#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "utils/assert.hpp"
#include "base_segment.hpp"
#include "dictionary_segment.hpp"
#include "table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

// ReferenceSegment is a specific segment type that stores all its values as position list of a referenced segment
class ReferenceSegment : public BaseSegment {
 public:
  // creates a reference segment
  // the parameters specify the positions and the referenced segment
  ReferenceSegment(const std::shared_ptr<const Table> referenced_table, const ColumnID referenced_column_id, const std::shared_ptr<const PosList> pos);

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  void append(const AllTypeVariant&) override { throw std::logic_error("ReferenceSegment is immutable"); };

  size_t size() const override;

  size_t estimate_memory_usage() const;

  const std::shared_ptr<const PosList> pos_list() const;
  const std::shared_ptr<const Table> referenced_table() const;

  ColumnID referenced_column_id() const;

 private:
  const std::shared_ptr<const Table> _referenced_table;
  ColumnID _referenced_column_id;
  const std::shared_ptr<const PosList> _pos_list;
};

}  // namespace opossum
