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
#include "storage/table.hpp"

namespace opossum {

  TableScan::TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
                       const AllTypeVariant search_value):
          			   _in(in), 
          			   _column_id(column_id), 
          			   _scan_type(scan_type), 
          			   _search_value(search_value), 
          			   _implementation(make_implementation(search_value)) {}
          
  std::unique_ptr<TableScan::TableScanImpl<AllTypeVariant>> TableScan::make_implementation(const AllTypeVariant &search_value) {
    const auto &type_name = _in->get_output()->column_type(_column_id);
    return make_unique_by_data_type<TableScan, TableScan::TableScanImpl<AllTypeVariant>>(type_name);
  }

  std::shared_ptr<const Table> TableScan::_on_execute() {
  	return _implementation->_on_execute();
  }

  template <typename T>
  TableScan::TableScanImpl<T>::TableScanImpl(const TableScan& table_scan) : _parent(table_scan) {}

  template <typename T>
  std::shared_ptr<const Table> TableScan::TableScanImpl<T>::_on_execute() {
  	return std::make_shared<const Table>();
  }

}  // namespace opossum
