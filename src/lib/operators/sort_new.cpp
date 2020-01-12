#include "sort_new.hpp"
#include <storage/segment_iterate.hpp>

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

std::vector<std::vector<AllTypeVariant>> SortNew::_get_filtered_rows() {
  auto table = _input_left->get_output();

  // Allocate all rows
  auto rows = std::vector<std::vector<AllTypeVariant>>{table->row_count()};
  const auto num_columns = _sort_definitions.size();
  for (auto& row : rows) {
    row.resize(num_columns);
  }

  // Materialize the Chunks
  auto chunk_begin_row_idx = size_t{0};
  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    if (!chunk) continue;

    for (auto column_id = ColumnID{0}; column_id < num_columns; ++column_id) {
      auto sort_definition = _sort_definitions[column_id];
      segment_iterate(*chunk->get_segment(sort_definition.column), [&](const auto& segment_position) {
        if (!segment_position.is_null()) {
          rows[chunk_begin_row_idx + segment_position.chunk_offset()][column_id] = segment_position.value();
        }
      });
    }

    chunk_begin_row_idx += chunk->size();
  }

  return rows;
}

}  // namespace opossum