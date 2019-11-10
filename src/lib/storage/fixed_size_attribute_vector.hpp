#pragma once

#include <vector>

#include "base_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class FixedSizeAttributeVector : public BaseAttributeVector {
 public:
  explicit FixedSizeAttributeVector(const size_t size) { _attribute_vector = std::vector<T>(size); }

  // returns the value id at a given position
  ValueID get(const size_t i) const override { return ValueID(_attribute_vector.at(i)); }

  // sets the value id at a given position
  void set(const size_t i, const ValueID value_id) override { _attribute_vector[i] = value_id; }

  // returns the number of values
  size_t size() const override { return _attribute_vector.size(); }

  // returns the width of biggest value id in bytes
  AttributeVectorWidth width() const override { return AttributeVectorWidth(sizeof(T)); }

 private:
  std::vector<T> _attribute_vector;
};

}  // namespace opossum
