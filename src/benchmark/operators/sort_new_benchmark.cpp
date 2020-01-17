#include <memory>

#include "benchmark/benchmark.h"

#include "../micro_benchmark_basic_fixture.hpp"
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "operators/limit.hpp"
#include "operators/sort_new.hpp"
#include "operators/table_wrapper.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_translator.hpp"
#include "synthetic_table_generator.hpp"

namespace opossum {

class SortNewBenchmark : public MicroBenchmarkBasicFixture {
 public:
  void BM_SortNew(benchmark::State& state) {
    _clear_cache();

    auto warm_up = std::make_shared<SortNew>(_table_wrapper_a, ColumnID{0} /* "a" */, OrderByMode::Ascending);
    warm_up->execute();
    for (auto _ : state) {
      auto sort = std::make_shared<SortNew>(_table_wrapper_a, ColumnID{0} /* "a" */, OrderByMode::Ascending);
      sort->execute();
    }
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
    // We only set _table_wrapper_a because this is the only table (currently) used in the SortNew Benchmarks
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
};

class SortNewPicoBenchmark : public SortNewBenchmark {
 public:
  void SetUp(benchmark::State& st) override { InitializeCustomTableWrapper(size_t{2}, ChunkOffset{2'000}); }
};

class SortNewSmallBenchmark : public SortNewBenchmark {
 public:
  void SetUp(benchmark::State& st) override { InitializeCustomTableWrapper(size_t{4'000}, ChunkOffset{2'000}); }
};

class SortNewLargeBenchmark : public SortNewBenchmark {
 public:
  void SetUp(benchmark::State& st) override { InitializeCustomTableWrapper(size_t{400'000}, ChunkOffset{2'000}); }
};

class SortNewReferencePicoBenchmark : public SortNewPicoBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    SortNewPicoBenchmark::SetUp(st);
    MakeReferenceTable();
  }
};

class SortNewReferenceSmallBenchmark : public SortNewSmallBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    SortNewSmallBenchmark::SetUp(st);
    MakeReferenceTable();
  }
};

class SortNewReferenceBenchmark : public SortNewBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    SortNewBenchmark::SetUp(st);
    MakeReferenceTable();
  }
};

class SortNewReferenceLargeBenchmark : public SortNewLargeBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    SortNewLargeBenchmark::SetUp(st);
    MakeReferenceTable();
  }
};

class SortNewStringSmallBenchmark : public SortNewBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    InitializeCustomTableWrapper(size_t{4'000}, ChunkOffset{2'000}, DataType::String);
  }
};

class SortNewStringBenchmark : public SortNewBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    InitializeCustomTableWrapper(size_t{40'000}, ChunkOffset{2'000}, DataType::String);
  }
};

class SortNewStringLargeBenchmark : public SortNewBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    InitializeCustomTableWrapper(size_t{400'000}, ChunkOffset{2'000}, DataType::String);
  }
};

class SortNewNullBenchmark : public SortNewBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    InitializeCustomTableWrapper(size_t{40'000}, ChunkOffset{2'000}, DataType::Int, std::nullopt,
                                 std::optional<float>{0.2});
  }
};


BENCHMARK_F(SortNewPicoBenchmark, BM_SortNewPico)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewSmallBenchmark, BM_SortNewSmall)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewBenchmark, BM_SortNew)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewLargeBenchmark, BM_SortNewLarge)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewReferencePicoBenchmark, BM_SortNewReferencePico)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewReferenceSmallBenchmark, BM_SortNewReferenceSmall)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewReferenceBenchmark, BM_SortNewReference)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewReferenceLargeBenchmark, BM_SortNewReferenceLarge)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewStringSmallBenchmark, BM_SortNewStringSmall)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewStringBenchmark, BM_SortNewString)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewStringLargeBenchmark, BM_SortNewStringLarge)(benchmark::State& state) { BM_SortNew(state); }

BENCHMARK_F(SortNewNullBenchmark, BM_SortNewNullBenchmark)(benchmark::State& state) { BM_SortNew(state); }


}  // namespace opossum
