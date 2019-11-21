#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_segment.hpp"
#include "fixed_size_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"
#include "value_segment.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseSegment;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   * Creates a Dictionary segment from a given value segment.
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment) {
    std::shared_ptr<ValueSegment<T>> value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(base_segment);

    // create dictionary
    _dictionary = std::make_shared<std::vector<T>>(value_segment->values());
    std::sort(_dictionary->begin(), _dictionary->end());
    // eliminates all but the first element from every consecutive group of equivalent elements
    auto last = std::unique(_dictionary->begin(), _dictionary->end());
    // the dictionary must be resized following the unique operation
    _dictionary->erase(last, _dictionary->end());

    // create Attribute Vector with the most fitting width
    _attribute_vector = _create_fix_sized_attribute_vector(_dictionary->size(), value_segment->size());

    // for each entry in the segment
    for (ChunkOffset segment_column(0); segment_column < value_segment->size(); ++segment_column) {
      // find corresponding dictionary value
      const ValueID& dictionary_value_id = lower_bound(type_cast<T>((*value_segment)[segment_column]));
      DebugAssert(dictionary_value_id != INVALID_VALUE_ID, "a dictionary value must be found for each attribute");
      _attribute_vector->set(segment_column, dictionary_value_id);
    }
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionarySegment in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override {
    PerformanceWarning("operator[] used");

    auto value_id = _attribute_vector->get(chunk_offset);
    return _dictionary->at(value_id);
  }

  // return the value at a certain position.
  T get(const size_t chunk_offset) const {
    auto value_id = _attribute_vector->get(chunk_offset);
    return _dictionary->at(value_id);
  }

  // dictionary segments are immutable
  void append(const AllTypeVariant&) override {
    throw std::runtime_error("Tried to append but Dictionary Segments are immutable");
  }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    auto lower_bound_it = std::lower_bound(_dictionary->begin(), _dictionary->end(), value);
    if (lower_bound_it == _dictionary->end()) {
      return INVALID_VALUE_ID;
    }
    return ValueID(std::distance(_dictionary->begin(), lower_bound_it));
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return lower_bound(type_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    auto upper_bound_it = std::upper_bound(_dictionary->begin(), _dictionary->end(), value);
    if (upper_bound_it == _dictionary->end()) {
      return INVALID_VALUE_ID;
    }
    return ValueID(std::distance(_dictionary->begin(), upper_bound_it));
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(type_cast<T>(value)); }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

  // returns the calculated memory usage
  size_t estimate_memory_usage() const final {
    return _attribute_vector->size() * _attribute_vector->width() + _dictionary->size() * sizeof(T);
  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;

 private:
  std::shared_ptr<BaseAttributeVector> _create_fix_sized_attribute_vector(const size_t dict_size,
                                                                          const size_t value_segment_size) {
    if (dict_size <= std::numeric_limits<uint8_t>::max()) {
      return std::make_shared<FixedSizeAttributeVector<uint8_t>>(value_segment_size);
    } else if (dict_size <= std::numeric_limits<uint16_t>::max()) {
      return std::make_shared<FixedSizeAttributeVector<uint16_t>>(value_segment_size);
    } else {
      return std::make_shared<FixedSizeAttributeVector<uint32_t>>(value_segment_size);
    }
  }
};

}  // namespace opossum
