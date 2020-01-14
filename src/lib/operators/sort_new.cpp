#include "sort_new.hpp"
#include <storage/segment_iterate.hpp>

namespace opossum {

SortNew::SortNew(const std::shared_ptr<const AbstractOperator>& in,
                 const std::vector<SortColumnDefinition>& sort_definitions, const size_t output_chunk_size)
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
                 const OrderByMode order_by_mode, const size_t output_chunk_size)
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

  // TODO(anyone): Materialize output, similar to Sort::SortImplMaterializeOutput
  auto output = _get_materialized_output(filtered_rows);

  return output;
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

std::shared_ptr<Table> SortNew::_get_materialized_output(
    std::vector<std::pair<RowID, std::vector<AllTypeVariant>>>& sorted_rows) {
  // First we create a new table as the output
  auto output =
      std::make_shared<Table>(_input_left->get_output()->column_definitions(), TableType::Data, _output_chunk_size);

  // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408

  // After we created the output table and initialized the column structure, we can start adding values. Because the
  // values are not ordered by input chunks anymore, we can't process them chunk by chunk. Instead the values are
  // copied column by column for each output row. For each column in a row we visit the input segment with a reference
  // to the output segment.
  const auto row_count_out = sorted_rows.size();

  // Ceiling of integer division
  const auto div_ceil = [](auto x, auto y) { return (x + y - 1u) / y; };

  const auto chunk_count_out = div_ceil(row_count_out, _output_chunk_size);

  // Vector of segments for each chunk
  std::vector<Segments> output_segments_by_chunk(chunk_count_out);

  // Materialize segment-wise
  for (ColumnID column_id{0u}; column_id < output->column_count(); ++column_id) {
    const auto column_data_type = output->column_data_type(column_id);

    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;

      auto chunk_it = output_segments_by_chunk.begin();
      auto chunk_offset_out = 0u;

      auto value_segment_value_vector = pmr_concurrent_vector<ColumnDataType>();
      auto value_segment_null_vector = pmr_concurrent_vector<bool>();

      value_segment_value_vector.reserve(row_count_out);
      value_segment_null_vector.reserve(row_count_out);

      auto segment_ptr_and_accessor_by_chunk_id =
          std::unordered_map<ChunkID, std::pair<std::shared_ptr<const BaseSegment>,
                                                std::shared_ptr<AbstractSegmentAccessor<ColumnDataType>>>>();
      segment_ptr_and_accessor_by_chunk_id.reserve(row_count_out);

      for (auto row_index = 0u; row_index < row_count_out; ++row_index) {
        const auto [chunk_id, chunk_offset] = sorted_rows.at(row_index).first;

        auto& segment_ptr_and_typed_ptr_pair = segment_ptr_and_accessor_by_chunk_id[chunk_id];
        auto& base_segment = segment_ptr_and_typed_ptr_pair.first;
        auto& accessor = segment_ptr_and_typed_ptr_pair.second;

        if (!base_segment) {
          base_segment = _input_left->get_output()->get_chunk(chunk_id)->get_segment(column_id);
          accessor = create_segment_accessor<ColumnDataType>(base_segment);
        }

        // If the input segment is not a ReferenceSegment, we can take a fast(er) path
        if (accessor) {
          const auto typed_value = accessor->access(chunk_offset);
          const auto is_null = !typed_value;
          value_segment_value_vector.push_back(is_null ? ColumnDataType{} : typed_value.value());
          value_segment_null_vector.push_back(is_null);
        } else {
          const auto value = (*base_segment)[chunk_offset];
          const auto is_null = variant_is_null(value);
          value_segment_value_vector.push_back(is_null ? ColumnDataType{} : boost::get<ColumnDataType>(value));
          value_segment_null_vector.push_back(is_null);
        }

        ++chunk_offset_out;

        // Check if value segment is full
        if (chunk_offset_out >= _output_chunk_size) {
          chunk_offset_out = 0u;
          auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                              std::move(value_segment_null_vector));
          chunk_it->push_back(value_segment);
          value_segment_value_vector = pmr_concurrent_vector<ColumnDataType>();
          value_segment_null_vector = pmr_concurrent_vector<bool>();
          ++chunk_it;
        }
      }

      // Last segment has not been added
      if (chunk_offset_out > 0u) {
        auto value_segment = std::make_shared<ValueSegment<ColumnDataType>>(std::move(value_segment_value_vector),
                                                                            std::move(value_segment_null_vector));
        chunk_it->push_back(value_segment);
      }
    });
  }

  for (auto& segments : output_segments_by_chunk) {
    output->append_chunk(segments);
  }

  return output;
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