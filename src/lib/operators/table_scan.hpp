#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/value_segment.hpp"
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

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  const std::shared_ptr<const AbstractOperator> _in;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

  template <typename T>
  void _scan_value_segment(const std::shared_ptr<PosList> pos_list, const std::function<bool(T, T)>& comparator,
                           const T typed_search_value, const ChunkID& chunk_index,
                           const std::shared_ptr<ValueSegment<T>> value_segment,
                           const std::vector<ChunkOffset>& input_filter) const;

  template <typename T>
  void _scan_dictionary_segment(const std::shared_ptr<PosList> pos_list, const T typed_search_value,
                                const ChunkID& chunk_index,
                                const std::shared_ptr<DictionarySegment<T>> dictionary_segment,
                                const std::vector<ChunkOffset>& input_filter) const;

  template <typename T>
  void _scan_reference_segment(const std::shared_ptr<PosList> pos_list, const std::function<bool(T, T)>& comparator,
                               const T typed_search_value,
                               const std::shared_ptr<ReferenceSegment> reference_segment) const;
};

}  // namespace opossum
