#include "sort_new.hpp"

namespace opossum {

SortNew::SortNew(const std::shared_ptr<const AbstractOperator>& in,
                 const std::vector<SortColumnDefinition>& sort_definitions, size_t output_chunk_size)
    : AbstractReadOnlyOperator(OperatorType::Sort, in),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size) {}

const std::vector<SortColumnDefinition>& SortNew::sort_definitions() const { return _sort_definitions; }

const std::string& SortNew::name() const {
  static const auto name = std::string{"SortNew"};
  return name;
}

std::shared_ptr<const Table> SortNew::_on_execute() {
  // TODO(anyone): Implement
  return _input_left->get_output();
}

}  // namespace opossum