#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "table_scan.hpp"

namespace opossum {

  TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                       const AllTypeVariant search_value):
          _implementation(make_implementation(search_value)) {};
          
  std::unique_ptr<TableScan::TableScanImpl<AllTypeVariant>> TableScan::make_implementation(const AllTypeVariant &search_value) {
    const auto &type_name = types[search_value.which()];
    return make_unique_by_data_type<AbstractOperator, TableScanImpl>(type_name);
  }

}  // namespace opossum
