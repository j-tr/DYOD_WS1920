#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TableScan;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan();

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  template <typename T>
  class TableScanImpl : public TableScan {
    public:
      TableScanImpl(const TableScan& table_scan);
      std::shared_ptr<const Table> _on_execute();
    private:
      const TableScan& _parent;
  };

  const std::shared_ptr<const AbstractOperator> _in;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

  const std::unique_ptr<TableScanImpl<AllTypeVariant>> _implementation;

  std::unique_ptr<TableScanImpl<AllTypeVariant>> make_implementation(const AllTypeVariant &search_value);
};

}  // namespace opossum
