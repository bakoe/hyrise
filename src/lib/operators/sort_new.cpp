#include "sort_new.hpp"

namespace opossum {

SortNew::SortNew(const std::shared_ptr<const AbstractOperator>& in,
                 const std::vector<SortColumnDefinition>& sort_definitions, size_t output_chunk_size)
    : AbstractReadOnlyOperator(OperatorType::Sort, in),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size) {}

SortNew::SortNew(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                 const OrderByMode order_by_mode, size_t output_chunk_size)
    : AbstractReadOnlyOperator(OperatorType::Sort, in),
      _sort_definitions({{column_id, order_by_mode}}),
      _output_chunk_size(output_chunk_size) {}

const std::vector<SortColumnDefinition>& SortNew::sort_definitions() const { return _sort_definitions; }

const std::string& SortNew::name() const {
  static const auto name = std::string{"SortNew"};
  return name;
}

std::shared_ptr<AbstractOperator> SortNew::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<SortNew>(copied_input_left, _sort_definitions, _output_chunk_size);
}

void SortNew::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> SortNew::_on_execute() {
  // TODO(anyone): Implement
  return _input_left->get_output();
}

void SortNew::_on_cleanup() {
  // TODO(anyone): Implement, if necessary
}

}  // namespace opossum