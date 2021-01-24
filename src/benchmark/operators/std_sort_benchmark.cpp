#include <algorithm>
#include <random>

#include "benchmark/benchmark.h"
#include "synthetic_table_generator.hpp"
#include "types.hpp"

#define SIZE 40'000

namespace opossum {

static void BM_StdBaseInt(benchmark::State& state) {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist;

  for (auto _ : state) {
    pmr_vector<int> v;
    v.reserve(state.range(0));
    for (int i = 0; i < state.range(0); i++) {
      v.push_back(dist(mt));
    }
  }
}
BENCHMARK(BM_StdBaseInt)->Arg(SIZE);

static void BM_StdSortInt(benchmark::State& state) {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist;

  for (auto _ : state) {
    pmr_vector<int> v;
    v.reserve(state.range(0));
    for (int i = 0; i < state.range(0); i++) {
      v.push_back(dist(mt));
    }

    std::sort(v.begin(), v.end());
  }
}
BENCHMARK(BM_StdSortInt)->Arg(SIZE);

static void BM_StdStableSortInt(benchmark::State& state) {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist;

  for (auto _ : state) {
    pmr_vector<int> v;
    v.reserve(state.range(0));
    for (int i = 0; i < state.range(0); i++) {
      v.push_back(dist(mt));
    }

    std::stable_sort(v.begin(), v.end());
  }
}
BENCHMARK(BM_StdStableSortInt)->Arg(SIZE);

static void BM_StdBasePmrString(benchmark::State& state) {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist;

  for (auto _ : state) {
    pmr_vector<pmr_string> v;
    v.reserve(state.range(0));
    for (int i = 0; i < state.range(0); i++) {
      v.push_back(SyntheticTableGenerator::generate_value<pmr_string>(dist(mt)));
    }
  }
}
BENCHMARK(BM_StdBasePmrString)->Arg(SIZE);

static void BM_StdSortPmrString(benchmark::State& state) {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist;

  for (auto _ : state) {
    pmr_vector<pmr_string> v;
    v.reserve(state.range(0));
    for (int i = 0; i < state.range(0); i++) {
      v.push_back(SyntheticTableGenerator::generate_value<pmr_string>(dist(mt)));
    }

    std::sort(v.begin(), v.end());
  }
}
BENCHMARK(BM_StdSortPmrString)->Arg(SIZE);

static void BM_StdStableSortPmrString(benchmark::State& state) {
  std::random_device rd;
  std::mt19937 mt(rd());
  std::uniform_int_distribution<int> dist;

  for (auto _ : state) {
    pmr_vector<pmr_string> v;
    v.reserve(state.range(0));
    for (int i = 0; i < state.range(0); i++) {
      v.push_back(SyntheticTableGenerator::generate_value<pmr_string>(dist(mt)));
    }

    std::stable_sort(v.begin(), v.end());
  }
}
BENCHMARK(BM_StdStableSortPmrString)->Arg(SIZE);

}  // namespace opossum
