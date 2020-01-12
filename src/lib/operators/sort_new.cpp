#include "sort_new.hpp"
#include <storage/segment_iterate.hpp>

namespace opossum {

SortNew::SortNew(const std::shared_ptr<const AbstractOperator>& in,
                 const std::vector<SortColumnDefinition>& sort_definitions, size_t output_chunk_size)
    : AbstractReadOnlyOperator(OperatorType::Sort, in),
      _sort_definitions(sort_definitions),
      _output_chunk_size(output_chunk_size) {
  _sort_definition_data_types.resize(_sort_definitions.size());
  for (auto column_id = ColumnID{0}; column_id < _sort_definitions.size(); ++column_id) {
    auto sort_definition = _sort_definitions[column_id];
    _sort_definition_data_types[column_id] = input_table_left()->column_data_type(sort_definition.column);
  }
}

SortNew::SortNew(const std::shared_ptr<const AbstractOperator>& in, const ColumnID column_id,
                 const OrderByMode order_by_mode, size_t output_chunk_size)
    : SortNew::SortNew(in, {{column_id, order_by_mode}}, output_chunk_size){};

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
  std::vector<std::pair<RowID, std::vector<AllTypeVariant>>> filtered_rows = _get_filtered_rows();

  std::stable_sort(filtered_rows.begin(), filtered_rows.end(),
                   [&](const std::pair<RowID, std::vector<AllTypeVariant>>& row_a,
                       const std::pair<RowID, std::vector<AllTypeVariant>>& row_b) {
                     for (auto column_id = ColumnID{0}; column_id < _sort_definitions.size(); ++column_id) {
                       auto sort_definition = _sort_definitions[column_id];
                       DataType data_type = _sort_definition_data_types[column_id];
                       OrderByMode order_by_mode = sort_definition.order_by_mode;
                       AllTypeVariant value_a = row_a.second[column_id];
                       AllTypeVariant value_b = row_b.second[column_id];
                       Assert((order_by_mode == OrderByMode::Ascending || order_by_mode == OrderByMode::Descending),
                              "Currently unsupported order_by_mode");

                       std::optional<bool> result;
                       resolve_data_type(data_type, [&](auto type) {
                         using ValueType = typename decltype(type)::type;

                         int8_t comparison_result = SortNew::_compare<ValueType>(boost::get<ValueType>(value_a),
                                                                                 boost::get<ValueType>(value_b));

                         if (comparison_result == 0) {
                           return;
                         }

                         if (order_by_mode == OrderByMode::Ascending) {
                           result = (comparison_result < 0);
                           return;
                         } else {
                           result = (comparison_result > 0);
                           return;
                         }
                       });

                       if (result.has_value()) {
                         return result.value();
                       }
                     }
                     // TODO(anyone): Return true or false depending on "last" order_by_mode to ensure stable-ness
                     return true;
                   });

  return _input_left->get_output();
}

template <typename ValueType>
int8_t SortNew::_compare(ValueType value_a, ValueType value_b) {
  if (value_a == value_b) {
    return 0;
  }
  if (value_a < value_b) {
    return -1;
  }
  return 1;
}

void SortNew::_on_cleanup() {
  // TODO(anyone): Implement, if necessary
}

std::vector<std::pair<RowID, std::vector<AllTypeVariant>>> SortNew::_get_filtered_rows() {
  auto table = _input_left->get_output();

  // Allocate all rows
  auto rows = std::vector<std::pair<RowID, std::vector<AllTypeVariant>>>{table->row_count()};
  const auto num_columns = _sort_definitions.size();
  for (auto& row : rows) {
    row.second.resize(num_columns);
  }

  // Materialize the Chunks
  auto chunk_begin_row_idx = size_t{0};
  const auto chunk_count = table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    if (!chunk) continue;

    for (auto column_id = ColumnID{0}; column_id < num_columns; ++column_id) {
      auto sort_definition = _sort_definitions[column_id];
      // TODO(anyone): Clarify: Is this inefficient, i. e. could it be improved by iterating row by row instead?
      segment_iterate(*chunk->get_segment(sort_definition.column), [&](const auto& segment_position) {
        RowID row_id = {chunk_id, segment_position.chunk_offset()};
        rows[chunk_begin_row_idx + segment_position.chunk_offset()].first = row_id;
        if (!segment_position.is_null()) {
          rows[chunk_begin_row_idx + segment_position.chunk_offset()].second[column_id] = segment_position.value();
        }
        // TODO(anyone): Handle NULL values appropriately
      });
    }

    chunk_begin_row_idx += chunk->size();
  }

  return rows;
}

}  // namespace opossum