#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "storage/value_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TableScan;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan() = default;

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

  template <typename T>
  std::vector<ChunkOffset> scan(std::shared_ptr<ValueSegment<T>> value_segment, std::function<bool (T, T)> comparator, const T search_value, std::vector<ChunkOffset> input_filter) const;

  template <typename T>
  std::vector<ChunkOffset> scan(std::shared_ptr<DictionarySegment<T>> dictionary_segment, std::function<bool (T, T)> comparator, const ValueID search_value_id, std::vector<ChunkOffset> input_filter) const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  const std::shared_ptr<const AbstractOperator> _in;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

  template <typename T>
  void _scan_value_segment(const std::shared_ptr<PosList>& pos_list, const std::function<bool(T, T)>& comparator,
                                 const T typed_search_value,
                                 const ChunkID& chunk_index,
                                 const std::shared_ptr<ValueSegment<T>>& value_segment) const;
};

}  // namespace opossum
