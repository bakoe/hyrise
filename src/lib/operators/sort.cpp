#include "sort.hpp"

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "operators/table_scan/abstract_table_scan_impl.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

Sort::Sort(const std::shared_ptr<const AbstractOperator>& in, const std::vector<OrderByDefinition> order_by_definitions,
           const size_t output_chunk_size)
    : AbstractReadOnlyOperator(OperatorType::Sort, in),
      _order_by_definitions(order_by_definitions),
      _output_chunk_size(output_chunk_size) {}

const std::string& Sort::name() const {
  static const auto name = std::string{"Sort"};
  return name;
}

std::shared_ptr<AbstractOperator> Sort::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<Sort>(copied_input_left, _order_by_definitions, _output_chunk_size);
}

void Sort::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> Sort::_on_execute() {
  std::vector<OrderByDefinitionWithDataType> order_by_definitions_with_data_types;
  order_by_definitions_with_data_types.reserve(_order_by_definitions.size());
  for (const auto& order_by_definition : _order_by_definitions) {
    const DataType column_data_type = input_table_left()->column_data_type(order_by_definition.column_id;

    /*
    resolve_data_type(column_data_type, [&](auto type) {
      using ColumnDataType = typename decltype(type)::type;
      OrderByDefinitionWithDataType<ColumnDataType> order_by_definitions_with_data_type;
      order_by_definitions_with_data_type.column_id = order_by_definition.column_id;
      order_by_definitions_with_data_type.order_by_mode = order_by_definition.order_by_mode;
      order_by_definitions_with_data_types.emplace_back(order_by_definitions_with_data_type);
    }
   */
    OrderByDefinitionWithDataType order_by_definitions_with_data_type;
    order_by_definitions_with_data_type.column_id = order_by_definition.column_id;
    order_by_definitions_with_data_type.order_by_mode = order_by_definition.order_by_mode;
    order_by_definitions_with_data_type.data_type = column_data_type;
    order_by_definitions_with_data_types.emplace_back(order_by_definitions_with_data_type);
  }
  /*
  _impl = make_unique_by_data_type<AbstractReadOnlyOperatorImpl, SortImpl>(
      input_table_left(), order_by_definitions_with_data_types, _output_chunk_size);
      */
  _impl = std::make_unique<SortImpl>(input_table_left(), order_by_definitions_with_data_types, _output_chunk_size);

  return _impl->_on_execute();
}

void Sort::_on_cleanup() { _impl.reset(); }

// This class fulfills only the materialization task for a sorted row_id_value_vector.
template <typename SortColumnType>
class Sort::SortImplMaterializeOutput {
 public:
  // creates a new table with reference segments
  SortImplMaterializeOutput(const std::shared_ptr<const Table>& in,
                            const std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>>& id_value_map,
                            const size_t output_chunk_size)
      : _table_in(in), _output_chunk_size(output_chunk_size), _row_id_value_vector(id_value_map) {}

  std::shared_ptr<Table> execute() {
    // First we create a new table as the output
    auto output = std::make_shared<Table>(_table_in->column_definitions(), TableType::Data, _output_chunk_size);

    // We have decided against duplicating MVCC data in https://github.com/hyrise/hyrise/issues/408

    // After we created the output table and initialized the column structure, we can start adding values. Because the
    // values are not ordered by input chunks anymore, we can't process them chunk by chunk. Instead the values are
    // copied column by column for each output row. For each column in a row we visit the input segment with a reference
    // to the output segment. This enables for the SortImplMaterializeOutput class to ignore the column types during the
    // copying of the values.
    const auto row_count_out = _row_id_value_vector->size();

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
          const auto [chunk_id, chunk_offset] = _row_id_value_vector->at(row_index).first;

          auto& segment_ptr_and_typed_ptr_pair = segment_ptr_and_accessor_by_chunk_id[chunk_id];
          auto& base_segment = segment_ptr_and_typed_ptr_pair.first;
          auto& accessor = segment_ptr_and_typed_ptr_pair.second;

          if (!base_segment) {
            base_segment = _table_in->get_chunk(chunk_id)->get_segment(column_id);
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

 protected:
  const std::shared_ptr<const Table> _table_in;
  const size_t _output_chunk_size;
  const std::shared_ptr<std::vector<std::pair<RowID, SortColumnType>>> _row_id_value_vector;
};

// TODO(anyone): Adjust this comment or refactor accordingly
// we need to use the impl pattern because the scan operator of the sort depends on the type of the column
class Sort::SortImpl {
 public:
  using RowIDValuesPair = std::pair<RowID, std::vector<AllTypeVariant>>;

  SortImpl(const std::shared_ptr<const Table>& table_in,
           const std::vector<OrderByDefinitionWithDataType> order_by_definitions_with_data_types,
           const size_t output_chunk_size = 0)
      : _table_in(table_in),
        _order_by_definitions_with_data_types(order_by_definitions_with_data_types),
        _output_chunk_size(output_chunk_size) {
    // initialize a structure which can be sorted by std::sort
    _row_id_values_vector = std::make_shared<std::vector<RowIDValuesPair>>();
    _null_values_rows = std::make_shared<std::vector<RowIDValuesPair>>();
  }

 protected:
  std::shared_ptr<const Table> _on_execute() override {
    // 1. Prepare Sort: Creating rowid-value-Structure
    _materialize_sort_column();

    // 2. After we got our ValueRowID Map we sort the map by the value of the pair
    if (_order_by_mode == OrderByMode::Ascending || _order_by_mode == OrderByMode::AscendingNullsLast) {
      _sort_with_operator<std::less<>>();
    } else {
      _sort_with_operator<std::greater<>>();
    }

    // 2b. Insert null rows if necessary
    if (!_null_value_rows->empty()) {
      if (_order_by_mode == OrderByMode::AscendingNullsLast || _order_by_mode == OrderByMode::DescendingNullsLast) {
        // NULLs last
        _row_id_value_vector->insert(_row_id_value_vector->end(), _null_value_rows->begin(), _null_value_rows->end());
      } else {
        // NULLs first (default behavior)
        _row_id_value_vector->insert(_row_id_value_vector->begin(), _null_value_rows->begin(), _null_value_rows->end());
      }
    }

    // 3. Materialization of the result: We take the sorted ValueRowID Vector, create chunks fill them until they are
    // full and create the next one. Each chunk is filled row by row.
    auto materialization = std::make_shared<SortImplMaterializeOutput<SortColumnType>>(_table_in, _row_id_value_vector,
                                                                                       _output_chunk_size);
    auto output = materialization->execute();

    const auto chunk_count = output->chunk_count();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      output->get_chunk(chunk_id)->set_ordered_by(std::make_pair(_column_id, _order_by_mode));
    }

    return output;
  }

  // completely materializes the sort column to create a vector of RowID-Value pairs
  void _materialize_sort_column() {
    auto& row_id_values_vector = *_row_id_values_vector;
    row_id_values_vector.resize(_table_in->row_count());
    for (auto& row_id_values : row_id_values_vector) {
      row_id_values.second = std::vector<AllTypeVariant>();
      row_id_values.second.resize(_order_by_definitions_with_data_types.size());
    }

    auto& null_value_rows = *_null_values_rows;
    null_value_rows.resize(_table_in->row_count());
    for (auto& null_value_row : null_value_rows) {
      null_value_row.second = std::vector<AllTypeVariant>();
      null_value_row.second.resize(_order_by_definitions_with_data_types.size());
    }

    auto chunk_begin_row_idx = size_t{0};
    const auto chunk_count = _table_in->chunk_count();
    for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = _table_in->get_chunk(chunk_id);
      Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

      for (auto definition_id = ColumnID{0}; definition_id < _order_by_definitions_with_data_types.size();
           definition_id++) {
        const auto& definition = _order_by_definitions_with_data_types[definition_id];
        auto base_segment = chunk->get_segment(definition.column_id);

        const DataType column_data_type = definition.data_type;
        resolve_data_type(column_data_type, [&](auto type) {
          using ColumnDataType = typename decltype(type)::type;
          segment_iterate<ColumnDataType>(*base_segment, [&](const auto& position) {
            if (position.is_null()) {
              std::pair<RowID, std::vector<AllTypeVariant>> row =
                  *null_value_rows[chunk_begin_row_idx + position.chunk_offset()];
              row.second[definition_id] = ColumnDataType{};
              // TODO Only set this once(?)
              row.first = RowID{chunk_id, position.chunk_offset()};
            } else {
              std::pair<RowID, std::vector<AllTypeVariant>> row =
                  *row_id_values_vector[chunk_begin_row_idx + position.chunk_offset()];
              row.second[definition_id] = position.value();
              // TODO Only set this once(?)
              row.first = RowID{chunk_id, position.chunk_offset()};
            }
          });
        });
      }

      chunk_begin_row_idx += chunk->size();
    }
  }

  template <typename Comparator>
  void _sort_with_operator() {
    Comparator comparator;
    std::stable_sort(_row_id_value_vector->begin(), _row_id_value_vector->end(),
                     [comparator](RowIDValuePair a, RowIDValuePair b) { return comparator(a.second, b.second); });
  }

  const std::shared_ptr<const Table> _table_in;
  const std::vector<OrderByDefinitionWithDataType> _order_by_definitions_with_data_types;
  // chunk size of the materialized output
  const size_t _output_chunk_size;

  std::shared_ptr<std::vector<RowIDValuesPair>> _row_id_values_vector;
  std::shared_ptr<std::vector<RowIDValuesPair>> _null_values_rows;
};

}  // namespace opossum
