/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import io.trino.spi.function.AggregationImplementation;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.ScalarFunctionImplementation;
import io.trino.spi.function.WindowFunctionSupplier;
import java.util.Map;


public class TransportFunctionProvider implements FunctionProvider {
  private final Map<FunctionId, StdUdfWrapper> functions;

  public TransportFunctionProvider(Map<FunctionId, StdUdfWrapper> functions) {
    this.functions = functions;
  }

  @Override
  public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionId functionId,
      BoundSignature boundSignature, FunctionDependencies functionDependencies,
      InvocationConvention invocationConvention) {
    return functions.get(functionId).getScalarFunctionImplementation(boundSignature, functionDependencies, invocationConvention);
  }

  @Override
  public AggregationImplementation getAggregationImplementation(FunctionId functionId, BoundSignature boundSignature,
      FunctionDependencies functionDependencies) {
    return null;
  }

  @Override
  public WindowFunctionSupplier getWindowFunctionSupplier(FunctionId functionId, BoundSignature boundSignature,
      FunctionDependencies functionDependencies) {
    return null;
  }
}
