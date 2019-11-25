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

/*
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
  }*/
}

namespace opossum {

  TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                       const AllTypeVariant search_value):
          			   _in(in), 
          			   _column_id(column_id), 
          			   _scan_type(scan_type), 
          			   _search_value(search_value) {}    

  template <typename T>
  std::vector<ChunkOffset> TableScan::scan(const std::shared_ptr<ValueSegment<T>> value_segment, const std::function<bool (T, T)> comparator, const T search_value, const std::vector<ChunkOffset> input_filter) const{
    std::vector<ChunkOffset> output_filter;
    const auto& values = value_segment->values();
    for(auto chunk_offset : input_filter) {
      const T& value = values[chunk_offset];
      if (comparator(value, search_value)) {
        output_filter.push_back(chunk_offset);
      }
    }
    return output_filter;
  }            

  template <typename T>
  std::vector<ChunkOffset> TableScan::scan(const std::shared_ptr<DictionarySegment<T>> dictionary_segment, const std::function<bool (T, T)> comparator, const T search_value, const std::vector<ChunkOffset> input_filter) const{
    std::vector<ChunkOffset> output_filter;
    // need to use lower and upper bound depeending on comparator...
    ValueID search_value_id = dictionary_segment->lower_bound(search_value);
    for(ColumnID attribute_vector_index{0}; attribute_vector_index < dictionary_segment->attribute_vector()->size(); ++attribute_vector_index) {
      if (comparator(dictionary_segment->attribute_vector()->get(attribute_vector_index), search_value_id)){
        output_filter.push_back(attribute_vector_index);
      }
    }
    return output_filter;
  }       

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

      for (ChunkID chunk_index = ChunkID(0); chunk_index < input_chunk_count; ++chunk_index) {
        // for each segment
          // - execute the comparison
          // - append to PosList
        const auto& segment = input_table->get_chunk(chunk_index).get_segment(_column_id);
        
        if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(segment)){
          // if segment is value segment
          // create dummy input filter that
          std::vector<ChunkOffset> input_filter(value_segment->size());
          std::iota(input_filter.begin(), input_filter.end(), 0);
          auto output_filter = scan(value_segment, comparator, typed_search_value, input_filter); 
          pos_list->reserve(pos_list->size() + output_filter.size());
          for (ChunkOffset chunk_offset : output_filter) {
            pos_list->push_back(RowID{chunk_index, chunk_offset});
          }
        } else if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(segment)){
          // if segment is dictionary segment
          std::vector<ChunkOffset> input_filter(value_segment->size());
          std::iota(input_filter.begin(), input_filter.end(), 0);
          auto output_filter = scan(dictionary_segment, comparator, typed_search_value, input_filter); 
          pos_list->reserve(pos_list->size() + output_filter.size());
          for (ChunkOffset chunk_offset : output_filter) {
            pos_list->push_back(RowID{chunk_index, chunk_offset});
          }
<<<<<<< HEAD
        } else if (const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment)){
          // if segment is reference segment
          // extract chunk_offsets for each referemced segment
          for (auto position_iterator = reference_segment->pos_list()->begin(); position_iterator < reference_segment->pos_list()->end();){
            
            ChunkID referenced_chunk_id = position_iterator->chunk_id;
            std::vector<ChunkOffset> input_filter;
            auto chunk_position_iterator = position_iterator;
            for (; chunk_position_iterator->chunk_id == position_iterator->chunk_id; chunk_position_iterator++){
              input_filter.push_back(chunk_position_iterator->chunk_offset);
            }
            position_iterator = chunk_position_iterator;

            auto referenced_segment = reference_segment->referenced_table()->get_chunk(referenced_chunk_id).get_segment(_column_id);
            if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<Type>>(referenced_segment)){
              // if referenced segment is value segment
              auto output_filter = scan(value_segment, comparator, typed_search_value, input_filter); 
              pos_list->reserve(pos_list->size() + output_filter.size());
              for (ChunkOffset chunk_offset : output_filter) {
                pos_list->push_back(RowID{referenced_chunk_id, chunk_offset});
              }
            } else if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<Type>>(referenced_segment)){
              // if referenced segment is dictionary segment
              auto output_filter = scan(value_segment, comparator, typed_search_value, input_filter); 
              pos_list->reserve(pos_list->size() + output_filter.size());
              for (ChunkOffset chunk_offset : output_filter) {
                pos_list->push_back(RowID{chunk_index, chunk_offset});
              }
            }   
          }
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

}  // namespace opossum
