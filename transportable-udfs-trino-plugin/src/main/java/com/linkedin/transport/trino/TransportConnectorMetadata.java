/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencyDeclaration;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.SchemaFunctionName;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Locale.ENGLISH;

/**
 * This class implements the interface of ConnectorMetadata from Trino SPI as a part of Trino plugin
 * to load UDF classes in Trino server following the development guideline
 * in https://trino.io/docs/current/develop/spi-overview.html
 */
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
    return functions.values().stream().map(StdUdfWrapper::getFunctionMetadata)
        // Function are expected to be called case agnostic
        .filter(e -> e.getCanonicalName().toLowerCase(ENGLISH).equals(name.getFunctionName()))
        .collect(Collectors.toList());
  }

  @Override
  public FunctionMetadata getFunctionMetadata(ConnectorSession session, FunctionId functionId) {
    return functions.get(functionId).getFunctionMetadata();
  }

  @Override
  public Collection<FunctionMetadata> listFunctions(ConnectorSession session, String schemaName) {
    return functions.values().stream().map(StdUdfWrapper::getFunctionMetadata).collect(toImmutableSet());
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession session) {
    return ImmutableList.of("transport");
  }
}
