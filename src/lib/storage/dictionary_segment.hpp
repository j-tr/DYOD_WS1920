#pragma once

#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "type_cast.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"

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
    // TODO find more intelligent way to create dictionary encoding
    //
    _dictionary = std::make_shared<std::vector<T>>(std::vector<T>());
    _attribute_vector = std::make_shared<std::vector<uint32_t>>(std::vector<uint32_t>());
    for (size_t i = 0; i < base_segment->size(); ++i) {
      T value = type_cast<T>((*base_segment)[i]);
      if(std::find(_dictionary->begin(), _dictionary->end(), value) == _dictionary->end()) {
        /* dictionary does not contain value */
        _dictionary->push_back(type_cast<T>((*base_segment)[i]));
      } 
    }   
    sort(_dictionary->begin(), _dictionary->end());

    for (size_t i = 0; i < base_segment->size(); ++i) {
      for (size_t j = 0; j < _dictionary->size(); ++j) {
        if (_dictionary->at(j) == type_cast<T>((*base_segment)[i])) {
          _attribute_vector->push_back(j);
        }
      }
    }   
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionarySegment in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override {
    PerformanceWarning("operator[] used");

    auto valueID = _attribute_vector->at(chunk_offset);
    return _dictionary->at(valueID);
  }

  // return the value at a certain position.
  T get(const size_t chunk_offset) const {
    auto valueID = _attribute_vector->at(chunk_offset);
    return _dictionary[valueID];
  }

  // dictionary segments are immutable
  void append(const AllTypeVariant&) override {
    // do nothing 
  }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const {
    return _dictionary; 
  }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const {
    return _attribute_vector;
  }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const {
    return _dictionary->at(value_id);
  }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    // TODO intelligent search algorithm
    for (auto it = _attribute_vector->begin(); it != _attribute_vector->end(); ++it) {
      auto dict_value = _dictionary->at(*(it));
      if (dict_value >= value) {
        return ValueID((*it));
      }
    }
    return INVALID_VALUE_ID;
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const {
    return lower_bound(type_cast<T>(value));
  }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    // TODO intelligent search algorithm
    for (auto it = _attribute_vector->begin(); it != _attribute_vector->end(); ++it) {
      auto dict_value = _dictionary->at(*it);
      if (dict_value > value) {
        return ValueID((*it));
      }
    }
    return INVALID_VALUE_ID;
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const {
    return upper_bound(type_cast<T>(value));
  }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const {
    return _dictionary->size();
  }

  // return the number of entries
  size_t size() const override {
    return _attribute_vector->size();
  }

  // returns the calculated memory usage
  size_t estimate_memory_usage() const final;

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  // std::shared_ptr<BaseAttributeVector> _attribute_vector;
  std::shared_ptr<std::vector<uint32_t>> _attribute_vector;
};

}  // namespace opossum
