#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager _instance;
  return _instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  _tables.insert(std::pair<std::string, std::shared_ptr<Table>>(name, table));
}

void StorageManager::drop_table(const std::string& name) {
  if (!_tables.erase(name)) {
    throw std::runtime_error("Table does not exist");
  }
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  return _tables.at(name);
}

bool StorageManager::has_table(const std::string& name) const {
  return _tables.find(name) != _tables.end();
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  for (auto& table : _tables) {
    names.push_back(table.first);
  }
  return names;
}

void StorageManager::print(std::ostream& out) const {
  for (auto& table : _tables) {
    out << table.first << " " << table.second->column_count() << " " << table.second->row_count() << " " << table.second->chunk_count() << std::endl;
  }
}

void StorageManager::reset() {
  _tables = std::map<std::string, std::shared_ptr<Table>>();
}

}  // namespace opossum
