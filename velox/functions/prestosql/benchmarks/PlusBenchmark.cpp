/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "folly/Random.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/CheckedArithmetic.h"

namespace facebook::velox::functions::test {

class AddBenchmark : public functions::test::FunctionBenchmarkBase {
public:
  AddBenchmark() : FunctionBenchmarkBase() {
	registerFunction<
		PlusFunction,
		int64_t,
		int64_t,
		int64_t>({"plus_unchecked"});

	registerFunction<
		CheckedPlusFunction,
		int64_t,
		int64_t,
		int64_t>({"plus_checked"});
  }

  VectorPtr makeData(bool withNulls) {
	constexpr vector_size_t size = 1000;

	return vectorMaker_.flatVector<int64_t>(
		size,
		[](auto row) { return row % 2 ? row : folly::Random::rand32() % size; },
		facebook::velox::test::VectorMaker::nullEvery(withNulls ? 5 : 0));
  }

  size_t run(const std::string &functionName, bool withNulls) {
	folly::BenchmarkSuspender suspender;
	auto inputs = vectorMaker_.rowVector({makeData(withNulls), makeData(withNulls)});

	auto exprSet = compileExpression(
		fmt::format("{}(c0, c1)", functionName), inputs->type());
	suspender.dismiss();
	return doRun(exprSet, inputs);
  }

  size_t doRun(exec::ExprSet &exprSet, const RowVectorPtr &rowVector) {
	int cnt = 0;
	for (auto i = 0; i < 100; i++) {
	  cnt += evaluate(exprSet, rowVector)->size();
	}
	folly::doNotOptimizeAway(cnt);
	return cnt;
  }
};

BENCHMARK_MULTI(PlusCheckedNullFree) {
  AddBenchmark benchmark;
  return benchmark.run("plus_checked", false);
}

BENCHMARK_MULTI(PlusCheckedWithNulls) {
  AddBenchmark benchmark;
  return benchmark.run("plus_checked", true);
}

BENCHMARK_MULTI(PlusUncheckedNullFree) {
  AddBenchmark benchmark;
  return benchmark.run("plus_unchecked", false);
}
}

int main(int /*argc*/, char ** /*argv*/) {
  folly::runBenchmarks();
  return 0;
}