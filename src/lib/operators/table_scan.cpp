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

  std::shared_ptr<const Table> TableScan::_on_execute() {
        
    // Get input table
    const auto input_table = _in->get_output();

    // get chunk count
    const ChunkID input_chunk_count = input_table->chunk_count();

    // create PosList
    const auto pos_list = std::make_shared<PosList>();

  	// cast all segments to ValueSegment or DictionarySegment
    const auto type_name = input_table->column_type(_column_id);

    resolve_data_type(type_name, [&] (auto type) {
      // Deduce type name of column type
      using Type = typename decltype(type)::type;
      const auto comparator = get_comparator<Type>(_scan_type);
      const auto typed_search_value = type_cast<Type>(_search_value);

      for (ChunkID chunk_index = ChunkID(0); chunk_index < input_chunk_count; ++chunk_index) {
        // for each segment
          // - execute the comparison
          // - append to PosList
          // 
        const auto& segment = input_table->get_chunk(chunk_index).get_segment(_column_id);
        
        // might be ValueSegment TODO or other segment (check by dynamic cast)
        if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment)){
          const auto& values = value_segment->values();
          const auto value_count = values.size();
          for(auto value_id = ValueID(0); value_id < value_count; value_id++) {
            const Type& value = values[value_id];
            if (comparator(value, typed_search_value)) {
              pos_list->emplace_back(chunk_index, value_id);
            }
          }
        } else if (const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment)){
          std::cout << "Implement me!" << std::endl;
        }
      }
    });

    // TODO: avoid to get another reference table as input_table
    auto result_table = std::make_shared<Table>(pos_list->size());
    auto chunk = std::make_shared<Chunk>();
    auto column_count = input_table->column_count();
    for(ColumnID column_id(0); column_id < column_count; ++column_id){
      chunk->add_segment(std::make_shared<ReferenceSegment>(input_table, column_id, pos_list));
      result_table->add_column(input_table->column_name(column_id), input_table->column_type(column_id));
    }
    
    result_table->emplace_chunk(chunk);
    return result_table;
  }

}  // namespace opossum
