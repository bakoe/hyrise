#include <iostream>
#include <memory>
#include <utility>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "operators/abstract_read_only_operator.hpp"
#include "operators/join_nested_loop.hpp"
#include "operators/print.hpp"
#include "operators/sort_new.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

class OperatorsSortNewTest : public BaseTestWithParam<EncodingType> {
 protected:
  void SetUp() override {
    _encoding_type = GetParam();

    _table_wrapper = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 2));
    _table_wrapper_null =
        std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float_with_null.tbl", 2));

    auto table = load_table("resources/test_data/tbl/int_float.tbl", 2);
    ChunkEncoder::encode_all_chunks(table, _encoding_type);

    auto table_dict = load_table("resources/test_data/tbl/int_float_with_null.tbl", 2);
    ChunkEncoder::encode_all_chunks(table_dict, _encoding_type);

    _table_wrapper_dict = std::make_shared<TableWrapper>(std::move(table));
    _table_wrapper_dict->execute();

    _table_wrapper_null_dict = std::make_shared<TableWrapper>(std::move(table_dict));
    _table_wrapper_null_dict->execute();

    _table_wrapper_outer_join = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float2.tbl", 2));
    _table_wrapper_outer_join->execute();

    _table_wrapper->execute();
    _table_wrapper_null->execute();
  }

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper, _table_wrapper_null, _table_wrapper_dict, _table_wrapper_null_dict,
      _table_wrapper_outer_join;
  EncodingType _encoding_type;
};

auto formatter = [](const ::testing::TestParamInfo<EncodingType> info) {
  return std::to_string(static_cast<uint32_t>(info.param));
};

// As long as two implementation of dictionary encoding exist, this ensure to run the tests for both.
INSTANTIATE_TEST_SUITE_P(DictionaryEncodingTypes, OperatorsSortNewTest, ::testing::Values(EncodingType::Dictionary),
                         formatter);

TEST_P(OperatorsSortNewTest, AscendingSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, AscendingSortOFilteredColumn) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_filtered_sorted.tbl", 2);

  auto input = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float.tbl", 1));
  input->execute();

  auto scan = create_table_scan(input, ColumnID{0}, PredicateCondition::NotEquals, 123);
  scan->execute();

  auto sort = std::make_shared<SortNew>(scan, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, AscendingSortOfOneColumnWithoutChunkSize) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper, ColumnID{0}, OrderByMode::Ascending);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, DoubleSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_sorted.tbl", 2);

  auto sort1 = std::make_shared<SortNew>(_table_wrapper, ColumnID{0}, OrderByMode::Descending, 2u);
  sort1->execute();

  auto sort2 = std::make_shared<SortNew>(sort1, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort2->execute();

  EXPECT_TABLE_EQ_ORDERED(sort2->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, DescendingSortOfOneColumn) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_reverse.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, MultipleColumnSortIsStable) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float4.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float2_sorted.tbl", 2);

  std::vector<SortColumnDefinition> sort_definitions = {{ColumnID{1}, OrderByMode::Ascending},
                                                        {ColumnID{0}, OrderByMode::Ascending}};
  auto sort = std::make_shared<SortNew>(table_wrapper, sort_definitions, 2u);

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, MultipleColumnSortIsStableMixedOrder) {
  auto table_wrapper = std::make_shared<TableWrapper>(load_table("resources/test_data/tbl/int_float4.tbl", 2));
  table_wrapper->execute();

  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float2_sorted_mixed.tbl", 2);

  std::vector<SortColumnDefinition> sort_definitions = {{ColumnID{1}, OrderByMode::Descending},
                                                        {ColumnID{0}, OrderByMode::Ascending}};
  auto sort = std::make_shared<SortNew>(table_wrapper, sort_definitions, 2u);

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, AscendingSortOfOneColumnWithNull) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper_null, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, DescendingSortOfOneColumnWithNull) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_null_sorted_desc.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper_null, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, AscendingSortOfOneColumnWithNullsLast) {
  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/int_float_null_sorted_asc_nulls_last.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper_null, ColumnID{0}, OrderByMode::AscendingNullsLast, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, DescendingSortOfOneColumnWithNullsLast) {
  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/int_float_null_sorted_desc_nulls_last.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper_null, ColumnID{0}, OrderByMode::DescendingNullsLast, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, AscendingSortOfOneDictSegmentWithNull) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_null_sorted_asc.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper_null_dict, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, DescendingSortOfOneDictSegmentWithNull) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_null_sorted_desc.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper_null_dict, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, AscendingSortOfOneDictSegment) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_sorted.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper_dict, ColumnID{0}, OrderByMode::Ascending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, DescendingSortOfOneDictSegment) {
  std::shared_ptr<Table> expected_result = load_table("resources/test_data/tbl/int_float_reverse.tbl", 2);

  auto sort = std::make_shared<SortNew>(_table_wrapper_dict, ColumnID{0}, OrderByMode::Descending, 2u);
  sort->execute();

  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

TEST_P(OperatorsSortNewTest, SortAfterOuterJoin) {
  auto join =
      std::make_shared<JoinNestedLoop>(_table_wrapper, _table_wrapper_outer_join, JoinMode::FullOuter,
                                       OperatorJoinPredicate{{ColumnID{0}, ColumnID{0}}, PredicateCondition::Equals});
  join->execute();

  auto sort = std::make_shared<SortNew>(join, ColumnID{0}, OrderByMode::Ascending);
  sort->execute();

  std::shared_ptr<Table> expected_result =
      load_table("resources/test_data/tbl/join_operators/int_outer_join_sorted_asc.tbl", 2);
  EXPECT_TABLE_EQ_ORDERED(sort->get_output(), expected_result);
}

}  // namespace opossum
