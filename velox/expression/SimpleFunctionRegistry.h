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

#pragma once

#include "velox/core/SimpleFunctionMetadata.h"
#include "velox/expression/FunctionRegistry.h"
#include "velox/expression/SimpleFunctionAdapter.h"

namespace facebook::velox::exec {

// The registry for simple functions.  These functions are converted to vector
// functions via the SimpleFunctionAdapter.
using SimpleFunctionRegistry = exec::FunctionRegistry<
    SimpleFunctionAdapterFactory,
    core::ISimpleFunctionMetadata>;

const SimpleFunctionRegistry& simpleFunctions();
SimpleFunctionRegistry& mutableSimpleFunctions();

// This function should be called once and alone.
template <typename UDFHolder>
void registerSimpleFunction(const std::vector<std::string>& names) {
  mutableSimpleFunctions()
      .registerFunction<SimpleFunctionAdapterFactoryImpl<UDFHolder>>(names);
}

} // namespace facebook::velox::exec
