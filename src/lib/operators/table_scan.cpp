#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <storage/reference_segment.hpp>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "table_scan.hpp"
#include "storage/table.hpp"

namespace /* anonymous */ {
  using namespace opossum;

  template <typename T>
  std::function<bool (T, T)> get_comparator(ScanType type) { // std::function<bool(T, T)>
    switch (type) {
      case ScanType::OpEquals:
        return [](T left, T right) -> bool { return left == right; };
      case ScanType::OpNotEquals:
        return [](T left, T right) -> bool { return left != right; };
      case ScanType::OpLessThan:
        return [](T left, T right) -> bool { return left < right; };
      case ScanType::OpLessThanEquals:
        return [](T left, T right) -> bool { return left <= right; };
      case ScanType::OpGreaterThan:
        return [](T left, T right) -> bool { return left > right; };
      case ScanType::OpGreaterThanEquals:
        return [](T left, T right) -> bool { return left >= right; };
      default:
        throw std::runtime_error("ScanType is not defined");
    }
  }
}

namespace opossum {

  TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                       const AllTypeVariant search_value):
          			   _in(in), 
          			   _column_id(column_id), 
          			   _scan_type(scan_type), 
          			   _search_value(search_value) {}
          
//  std::unique_ptr<TableScan::TableScanImpl<AllTypeVariant>> TableScan::make_implementation(
//          const AllTypeVariant &search_value, const std::shared_ptr<const AbstractOperator> in) {
//    const auto& type_name = _in->get_output()->column_type(_column_id);
//    return make_unique_by_data_type<AbstractOperator, TableScan::TableScanImpl<AllTypeVariant>>(type_name);
//  }

  std::shared_ptr<const Table> TableScan::_on_execute() {
        
    // Get input table
    const auto input_table = _in->get_output();
    // get chunks of input value
    const ChunkID input_chunk_count = input_table->chunk_count();
    auto input_segments = std::vector<std::shared_ptr<BaseSegment>>(input_chunk_count);
    // get (column) segment of each chunk
    for (ChunkID chunk_index = ChunkID(0); chunk_index < input_chunk_count; chunk_index++) {
      const auto& chunk = input_table->get_chunk(chunk_index);
      input_segments[chunk_index] = chunk.get_segment(_column_id);
    }
  	
    auto referenceSegments = std::vector<ReferenceSegment>();
            
  	// cast all segments to ValueSegment or DictionarySegment
    const auto type_name = input_table->column_type(_column_id);
    resolve_data_type(type_name, [&] (auto type) {
      // Deduce type name of column type
      using Type = typename decltype(type)::type;
      const auto comparator = get_comparator<Type>(_scan_type);
      const auto typed_search_value = type_cast<Type>(_search_value);

      for (ChunkID chunk_index = ChunkID(0); chunk_index < input_chunk_count; chunk_index++) {
        const auto& segment = input_segments[chunk_index];
        // might be ValueSegment TODO or other segment
        const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment);
        if (value_segment != nullptr) {
          const auto pos_list = std::make_shared<PosList>();
          const auto& values = value_segment->values();
          const auto value_count = values.size();
          for(auto value_id = ValueID(0); value_id < value_count; value_id++) {
            const Type& value = values[value_id];
            if (comparator(value, typed_search_value)) {
              pos_list->emplace_back(chunk_index, value_id);
            }
          }
          // for each segment
          // - create PosList
          // - execute the comparison
          // 
          // push_back ReferenceSegment based on PosList
          if (!pos_list->empty()) {
            const auto& table = input_table; // TODO use acutal original table to avoid indirection
            referenceSegments.emplace_back(table, _column_id, pos_list);
          }
        }
      }
    });

    auto result_table = std::make_shared<Table>(input_table->max_chunk_size());
    for (ChunkID chunk_index = ChunkID(0); chunk_index < input_chunk_count; chunk_index++) {
      const auto chunk = std::make_shared<Chunk>();
      result_table->emplace_chunk(chunk);
    }
    return result_table;
  }

  template <typename T>
  TableScan::TableScanImpl<T>::TableScanImpl(const TableScan& table_scan) : _parent(table_scan) {}

  template <typename T>
  std::shared_ptr<const Table> TableScan::TableScanImpl<T>::_on_execute() {
  	return std::make_shared<const Table>();
  }

}  // namespace opossum
