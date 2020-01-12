#pragma once

#include "abstract_read_only_operator.hpp"

namespace opossum {

/**
 * TODO(anyone): Add documentation comment, similar to the one of AggregateColumnDefinition
 */
struct SortColumnDefinition final {
  SortColumnDefinition(const ColumnID& column, const OrderByMode order_by_mode = OrderByMode::Ascending)
      : column(column), order_by_mode(order_by_mode) {}

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

  const std::vector<SortColumnDefinition>& sort_definitions() const;

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  // TODO(anyone): Enable and implement the following for sort definition validation (see abstract_aggregate_operator)
  // void _validate_sort_definitions() const;

  // TODO(anyone): Add and implement other "boilerplate" methods like _on_cleanup, _on_deep_copy etc.

  const std::vector<SortColumnDefinition> _sort_definitions;
  const size_t _output_chunk_size;
};

}  // namespace opossum