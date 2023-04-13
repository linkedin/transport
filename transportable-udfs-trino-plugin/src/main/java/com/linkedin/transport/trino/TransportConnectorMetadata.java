/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;


public class TransportConnectorMetadata implements ConnectorMetadata {
  private final Map<FunctionId, StdUdfWrapper> functions;

  public TransportConnectorMetadata(Map<FunctionId, StdUdfWrapper> functions) {
    this.functions = functions;
  }

  @Override
  public FunctionDependencyDeclaration getFunctionDependencies(ConnectorSession session, FunctionId functionId,
      BoundSignature boundSignature) {
    return functions.get(functionId).getFunctionDependencies(boundSignature);
  }

  @Override
  public Collection<FunctionMetadata> getFunctions(ConnectorSession session, SchemaFunctionName name) {
    return functions.values().stream().filter(e -> e.getFunctionMetadata().getCanonicalName().equals(name.getFunctionName()))
        .map(StdUdfWrapper::getFunctionMetadata).collect(Collectors.toList());
  }

  @Override
  public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId) {
    return functions.get(functionId).getFunctionMetadata();
  }
}
