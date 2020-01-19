#include <memory>

#include "benchmark/benchmark.h"

#include "../micro_benchmark_basic_fixture.hpp"
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "operators/limit.hpp"
#include "operators/sort.hpp"
#include "operators/sort_new.hpp"
#include "operators/table_wrapper.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_translator.hpp"
#include "synthetic_table_generator.hpp"

#include "micro_benchmark_utils.hpp"

namespace opossum {

static std::shared_ptr<Table> generate_custom_table(
    const size_t row_count, const ChunkOffset chunk_size, const DataType data_type = DataType::Int,
    const std::optional<std::vector<std::string>>& column_names = std::nullopt,
    const std::optional<float> null_ratio = std::nullopt) {
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();

  size_t num_columns = 2;
  if (column_names.has_value()) {
    num_columns = column_names.value().size();
  }

  const int max_different_value = 10'000;
  const std::vector<DataType> column_data_types = {num_columns, data_type};

  return table_generator->generate_table(
      {num_columns, {ColumnDataDistribution::make_uniform_config(0.0, max_different_value)}}, column_data_types,
      row_count, chunk_size, std::vector<SegmentEncodingSpec>(num_columns, {EncodingType::Unencoded}), column_names,
      UseMvcc::Yes, null_ratio);
}

template <class SortOperator>
static void BM_Sort(benchmark::State& state, const size_t row_count = 40'000, const ChunkOffset chunk_size = 2'000,
                    const DataType data_type = DataType::Int,
                    const std::optional<std::vector<std::string>>& column_names = std::nullopt,
                    const std::optional<float> null_ratio = std::nullopt, const bool multi_column_sort = true) {
  micro_benchmark_clear_cache();

  auto table_wrapper =
      std::make_shared<TableWrapper>(generate_custom_table(row_count, chunk_size, data_type, column_names, null_ratio));
  table_wrapper->execute();
  auto warm_up = std::make_shared<SortOperator>(table_wrapper, ColumnID{0} /* "a" */, OrderByMode::Ascending);
  warm_up->execute();
  for (auto _ : state) {
    if (multi_column_sort) {
      auto sort_a = std::make_shared<SortOperator>(table_wrapper, ColumnID{0} /* "a" */, OrderByMode::Ascending);
      sort_a->execute();
      auto sort_b = std::make_shared<SortOperator>(sort_a, ColumnID{1} /* "a" */, OrderByMode::Descending);
      sort_b->execute();
    } else {
      auto sort = std::make_shared<SortOperator>(table_wrapper, ColumnID{0} /* "a" */, OrderByMode::Ascending);
      sort->execute();
    }
  }
}

template <class SortOperator>
static void BM_SortNew(benchmark::State& state, const size_t row_count = 40'000, const ChunkOffset chunk_size = 2'000,
                       const DataType data_type = DataType::Int,
                       const std::optional<std::vector<std::string>>& column_names = std::nullopt,
                       const std::optional<float> null_ratio = std::nullopt, const bool multi_column_sort = true) {
  micro_benchmark_clear_cache();

  auto table_wrapper =
      std::make_shared<TableWrapper>(generate_custom_table(row_count, chunk_size, data_type, column_names, null_ratio));
  table_wrapper->execute();
  auto warm_up = std::make_shared<SortOperator>(table_wrapper, ColumnID{0} /* "a" */, OrderByMode::Ascending);
  warm_up->execute();
  for (auto _ : state) {
    if (multi_column_sort) {
      std::vector<SortColumnDefinition> sort_definitions = {{ColumnID{1}, OrderByMode::Descending},
                                                            {ColumnID{0}, OrderByMode::Ascending}};
      auto sort = std::make_shared<SortOperator>(table_wrapper, sort_definitions);
      sort->execute();
    } else {
      auto sort = std::make_shared<SortOperator>(table_wrapper, ColumnID{0} /* "a" */, OrderByMode::Ascending);
      sort->execute();
    }
  }
}

static void BM_SortNewWithVaryingRowCount(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_SortNew<SortNew>(state, row_count);
}

static void BM_SortWithVaryingRowCount(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort<Sort>(state, row_count);
}

class SortBenchmark : public MicroBenchmarkBasicFixture {
 public:
  void BM_Sort(benchmark::State& state) {
    _clear_cache();

    auto warm_up = std::make_shared<Sort>(_table_wrapper_a, ColumnID{0} /* "a" */, OrderByMode::Ascending);
    warm_up->execute();
    for (auto _ : state) {
      auto sort = std::make_shared<Sort>(_table_wrapper_a, ColumnID{0} /* "a" */, OrderByMode::Ascending);
      sort->execute();
    }
  }

  void BM_SortSingleColumnSQL(benchmark::State& state) {
    const std::string query = R"(
        SELECT *
        FROM table_a
        ORDER BY col_1)";

    RunSQLBasedBenchmark(state, query);
  }

  void BM_SortMultiColumnSQL(benchmark::State& state) {
    const std::string query = R"(
        SELECT *
        FROM table_a
        ORDER BY col_1, col_2)";

    RunSQLBasedBenchmark(state, query);
  }

 protected:
  std::shared_ptr<Table> GenerateCustomTable(const size_t row_count, const ChunkOffset chunk_size,
                                             const DataType data_type = DataType::Int,
                                             const std::optional<std::vector<std::string>>& column_names = std::nullopt,
                                             const std::optional<float> null_ratio = std::nullopt) {
    const auto table_generator = std::make_shared<SyntheticTableGenerator>();

    size_t num_columns = 1;
    if (column_names.has_value()) {
      num_columns = column_names.value().size();
    }

    const int max_different_value = 10'000;
    const std::vector<DataType> column_data_types = {num_columns, data_type};

    return table_generator->generate_table(
        {num_columns, {ColumnDataDistribution::make_uniform_config(0.0, max_different_value)}}, column_data_types,
        row_count, chunk_size, std::vector<SegmentEncodingSpec>(num_columns, {EncodingType::Unencoded}), column_names,
        UseMvcc::Yes, null_ratio);
  }

  void InitializeCustomTableWrapper(const size_t row_count, const ChunkOffset chunk_size,
                                    const DataType data_type = DataType::Int,
                                    const std::optional<std::vector<std::string>>& column_names = std::nullopt,
                                    const std::optional<float> null_ratio = std::nullopt) {
    // We only set _table_wrapper_a because this is the only table (currently) used in the Sort Benchmarks
    _table_wrapper_a =
        std::make_shared<TableWrapper>(GenerateCustomTable(row_count, chunk_size, data_type, column_names, null_ratio));
    _table_wrapper_a->execute();
  }

  void MakeReferenceTable() {
    auto limit = std::make_shared<Limit>(_table_wrapper_a,
                                         expression_functional::to_expression(std::numeric_limits<int64_t>::max()));
    limit->execute();
    _table_wrapper_a = std::make_shared<TableWrapper>(limit->get_output());
    _table_wrapper_a->execute();
  }

  void RunSQLBasedBenchmark(benchmark::State& state, const std::string& query) {
    _clear_cache();

    auto& storage_manager = Hyrise::get().storage_manager;
    auto column_names = std::optional<std::vector<std::string>>(1);
    column_names->push_back("col_1");
    column_names->push_back("col_2");
    storage_manager.add_table("table_a",
                              GenerateCustomTable(size_t{40'000}, ChunkOffset{2'000}, DataType::Int, column_names));

    for (auto _ : state) {
      hsql::SQLParserResult result;
      hsql::SQLParser::parseSQLString(query, &result);
      auto result_node = SQLTranslator{UseMvcc::No}.translate_parser_result(result)[0];
      const auto pqp = LQPTranslator{}.translate_node(result_node);
      const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
    }
  }
};

class SortPicoBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { InitializeCustomTableWrapper(size_t{2}, ChunkOffset{2'000}); }
};

class SortSmallBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { InitializeCustomTableWrapper(size_t{4'000}, ChunkOffset{2'000}); }
};

class SortLargeBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { InitializeCustomTableWrapper(size_t{400'000}, ChunkOffset{2'000}); }
};

BENCHMARK(BM_SortWithVaryingRowCount)->RangeMultiplier(10)->Range(4, 400'000);
BENCHMARK(BM_SortNewWithVaryingRowCount)->RangeMultiplier(10)->Range(4, 400'000);

}  // namespace opossum
