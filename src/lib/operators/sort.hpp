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

struct OrderByDefinition {
  ColumnID column_id;
  OrderByMode order_by_mode;
};

struct OrderByDefinitionWithDataType {
  ColumnID column_id;
  OrderByMode order_by_mode;
  AllTypeVariant value;
  DataType data_type;
};

/**
 * Operator to sort a table by a single column. This implements a stable sort, i.e., rows that share the same value will
 * maintain their relative order.
 * Multi-column sort is not supported yet. For now, you will have to sort by the secondary criterion, then by the first
 */
class Sort : public AbstractReadOnlyOperator {
 public:
  // The parameter chunk_size sets the chunk size of the output table, which will always be materialized
  Sort(const std::shared_ptr<const AbstractOperator>& in, const std::vector<OrderByDefinition> order_by_definitions,
       const size_t output_chunk_size = Chunk::DEFAULT_SIZE);

  const std::string& name() const override;

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  void _on_cleanup() override;
  std::shared_ptr<AbstractOperator> _on_deep_copy(
      const std::shared_ptr<AbstractOperator>& copied_input_left,
      const std::shared_ptr<AbstractOperator>& copied_input_right) const override;
  void _on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) override;

  // The operator is separated in three different classes. SortImpl is the common templated implementation of the
  // operator. SortImpl* und SortImplMaterializeOutput are extra classes for the visitor pattern. They fulfill a certain
  // task during the Sort process, as described later on.
  class SortImpl;
  template <typename... SortColumnTypes>
  class SortImplMaterializeOutput;

  std::unique_ptr<SortImpl> _impl;
  const std::vector<OrderByDefinition> _order_by_definitions;
  const size_t _output_chunk_size;
};

}  // namespace opossum
