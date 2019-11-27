#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <numeric>
#include <storage/value_segment.hpp>
#include <storage/reference_segment.hpp>
#include <storage/dictionary_segment.hpp>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "table_scan.hpp"
#include "storage/table.hpp"
#include "operators/print.hpp"

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

  template <>
  std::function<bool (std::string, std::string)> get_comparator(ScanType type) { // std::function<bool(T, T)>
      switch (type) {
      case ScanType::OpEquals:
        return [](std::string left, std::string right) -> bool { return (left.compare(right) == 0); };
      case ScanType::OpNotEquals:
        return [](std::string left, std::string right) -> bool { return (left.compare(right) != 0); };
      case ScanType::OpLessThan:
        return [](std::string left, std::string right) -> bool { return (left.compare(right) < 0); };
      case ScanType::OpLessThanEquals:
        return [](std::string left, std::string right) -> bool { return (left.compare(right) <= 0); };
      case ScanType::OpGreaterThan:
        return [](std::string left, std::string right) -> bool { return (left.compare(right) > 0); };
      case ScanType::OpGreaterThanEquals:
        return [](std::string left, std::string right) -> bool { return (left.compare(right) >= 0); };
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
    const std::shared_ptr<const Table> input_table = _in->get_output();

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

      // for each chunk and the relevant segment, compare the values and append to PosList
      for (ChunkID chunk_index = ChunkID(0); chunk_index < input_chunk_count; ++chunk_index) {
        const auto& segment = input_table->get_chunk(chunk_index).get_segment(_column_id);

        if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment)){
          // if segment is value segment
          // create a PosList-equivalent vector with every possible chunk_offset
          std::vector<ChunkOffset> input_filter(value_segment->size());
          // fill the input_filter vector with sequentially increasing values
          std::iota(input_filter.begin(), input_filter.end(), 0);
          _scan_value_segment(pos_list, comparator, typed_search_value, chunk_index, value_segment, input_filter);
        } else if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment)){
          // if segment is dictionary segment
          std::vector<ChunkOffset> input_filter(dictionary_segment->size());
          std::iota(input_filter.begin(), input_filter.end(), 0);
          _scan_dictionary_segment(pos_list, typed_search_value, chunk_index, dictionary_segment,
                                   input_filter);
        } else if (const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment)){
          // if segment is reference segment
          _scan_reference_segment(pos_list, comparator, typed_search_value, reference_segment);
        }
      }
    });
    
    auto result_table = std::make_shared<Table>(pos_list->size());
    auto chunk = std::make_shared<Chunk>();
    auto column_count = input_table->column_count();
    for(ColumnID column_id(0); column_id < column_count; ++column_id){
      result_table->add_column(input_table->column_name(column_id), input_table->column_type(column_id));
      chunk->add_segment(std::make_shared<ReferenceSegment>(input_table, column_id, pos_list));
    }

    result_table->emplace_chunk(chunk);
    return result_table;
  }

  template <typename T>
  void TableScan::_scan_reference_segment(const std::shared_ptr<PosList> pos_list,
                                          const std::function<bool(T, T)> &comparator,
                                          const T typed_search_value,
                                          const std::shared_ptr<ReferenceSegment> reference_segment) const {
    // for each position referenced in this segment, extract chunk_offsets
    for (auto position_iterator = reference_segment->pos_list()->begin(); position_iterator < reference_segment->pos_list()->end();){

      ChunkID referenced_chunk_id = position_iterator->chunk_id;
      auto chunk_position_iterator = position_iterator;
      auto input_filter = std::vector<ChunkOffset>{};
      for (; chunk_position_iterator->chunk_id == position_iterator->chunk_id; chunk_position_iterator++){
        input_filter.push_back(chunk_position_iterator->chunk_offset);
      }
      position_iterator = chunk_position_iterator;

      auto referenced_segment = reference_segment->referenced_table()->get_chunk(referenced_chunk_id).get_segment(
              _column_id);
      if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(referenced_segment)){
        // if referenced segment is value segment
        _scan_value_segment(pos_list, comparator, typed_search_value, referenced_chunk_id, value_segment, input_filter);
      } else if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(referenced_segment)){
        // if referenced segment is dictionary segment
        _scan_dictionary_segment(pos_list, typed_search_value,
                referenced_chunk_id, dictionary_segment, input_filter);
      }
    }
  }

  template <typename T>
  void TableScan::_scan_value_segment(const std::shared_ptr<PosList> pos_list,
                                      const std::function<bool(T, T)>& comparator,
                                      const T typed_search_value,
                                      const ChunkID& chunk_index,
                                      const std::shared_ptr<ValueSegment<T>> value_segment,
                                      const std::vector<ChunkOffset>& input_filter) const {
    std::vector<ChunkOffset> output_filter;
    const auto& values = value_segment->values();
    for(auto chunk_offset : input_filter) {
      const T& value = values[chunk_offset];
      if (comparator(value, typed_search_value)) {
        output_filter.push_back(chunk_offset);
      }
    }
    pos_list->reserve(pos_list->size() + output_filter.size());
    for (ChunkOffset chunk_offset : output_filter) {
      pos_list->push_back(RowID{chunk_index, chunk_offset});
    }
  }

  template <typename T>
  void TableScan::_scan_dictionary_segment(const std::shared_ptr<PosList> pos_list,
                                           const T typed_search_value,
                                           const ChunkID &chunk_index,
                                           const std::shared_ptr<DictionarySegment<T>> dictionary_segment,
                                           const std::vector<ChunkOffset> &input_filter) const {
    std::vector<ChunkOffset> output_filter;
    auto dictionary = dictionary_segment->dictionary();
    auto attribute_vector = dictionary_segment->attribute_vector();
    
    if (_scan_type == ScanType::OpNotEquals || _scan_type == ScanType::OpEquals){
      ValueID search_value_id = dictionary_segment->lower_bound(typed_search_value);
      if (search_value_id != INVALID_VALUE_ID && (dictionary->at(attribute_vector->get(search_value_id)) == typed_search_value)) {
        auto comparator = get_comparator<ValueID>(_scan_type);
        for(auto chunk_offset : input_filter) {
          if (comparator(attribute_vector->get(chunk_offset), search_value_id)) {
            output_filter.push_back(chunk_offset);
          }
        }
      } else {
        if (_scan_type == ScanType::OpNotEquals) {
          output_filter.resize(dictionary_segment->size());
          std::iota(output_filter.begin(), output_filter.end(), 0); 
        }
      }
    } else if (_scan_type == ScanType::OpGreaterThanEquals || _scan_type == ScanType::OpLessThan) {
      ValueID search_value_id = dictionary_segment->lower_bound(typed_search_value);
      if (search_value_id != INVALID_VALUE_ID){
        auto comparator = get_comparator<ValueID>(_scan_type);
        for(auto chunk_offset : input_filter) {
          if (comparator(attribute_vector->get(chunk_offset), search_value_id)) {
            output_filter.push_back(chunk_offset);
          }
        }
      } else {
        if (_scan_type == ScanType::OpLessThan) {
          output_filter.resize(dictionary_segment->size());
          std::iota(output_filter.begin(), output_filter.end(), 0); 
        }
      }
    } else if (_scan_type == ScanType::OpGreaterThan){
      ValueID search_value_id = dictionary_segment->upper_bound(typed_search_value);
      if (search_value_id != INVALID_VALUE_ID){
        for(auto chunk_offset : input_filter) {
          if (attribute_vector->get(chunk_offset) >= search_value_id) {
            output_filter.push_back(chunk_offset);
          }
        }
      }
    } else if (_scan_type == ScanType::OpLessThanEquals) {
      ValueID search_value_id = dictionary_segment->upper_bound(typed_search_value);
      if (search_value_id != INVALID_VALUE_ID){
        for(auto chunk_offset : input_filter) {
          if (attribute_vector->get(chunk_offset) < search_value_id) {
            output_filter.push_back(chunk_offset);
          }
        }
      } else { 
        output_filter.resize(dictionary_segment->size());
        std::iota(output_filter.begin(), output_filter.end(), 0); 
      }
    }
    
    pos_list->reserve(pos_list->size() + output_filter.size());
    for (ChunkOffset chunk_offset : output_filter) {
      pos_list->push_back(RowID{chunk_index, chunk_offset});
    }
  }

}  // namespace opossum
