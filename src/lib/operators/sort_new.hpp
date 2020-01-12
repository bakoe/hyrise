#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abstract_read_only_operator.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "types.hpp"

namespace opossum {

/**
 * TODO(anyone): Add documentation comment, similar to the one of AggregateColumnDefinition
 */
struct SortColumnDefinition final {
  SortColumnDefinition(const ColumnID& column, const OrderByMode order_by_mode = OrderByMode::Ascending)
      : column(column), order_by_mode(order_by_mode) {}

  // TODO(anyone): Maybe rename here and in AggregateColumnDefinition to column_id
  const ColumnID column;
  const OrderByMode order_by_mode;
};

/**
 * TODO(anyone): Add documentation comment
 */
class SortNew : public AbstractReadOnlyOperator {
 public:
  SortNew(const std::shared_ptr<const AbstractOperator>& in, const std::vector<SortColumnDefinition>& sort_definitions,
          const size_t output_chunk_size = Chunk::DEFAULT_SIZE);

  SortNew(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
          const OrderByMode order_by_mode = OrderByMode::Ascending,
          const size_t output_chunk_size = Chunk::DEFAULT_SIZE);

  const std::vector<SortColumnDefinition>& sort_definitions() const;

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  // TODO(anyone): Copied from Table::get_rows; maybe move into Table implementation or factor out util/helper function
  std::vector<std::pair<RowID, std::vector<AllTypeVariant>>> _get_filtered_rows();

  template <typename ValueType>
  int8_t _compare(ValueType value_a, ValueType value_b);

  // TODO(anyone): Enable and implement the following for sort definition validation (see abstract_aggregate_operator)
  // void _validate_sort_definitions() const;

  // TODO(anyone): Add and implement other "boilerplate" methods like _on_cleanup, _on_deep_copy etc.

  const std::vector<SortColumnDefinition> _sort_definitions;
  std::vector<DataType> _sort_definition_data_types;

  const size_t _output_chunk_size;
};

}  // namespace opossum