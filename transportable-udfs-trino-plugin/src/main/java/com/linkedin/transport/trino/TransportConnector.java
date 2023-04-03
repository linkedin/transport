/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.transaction.IsolationLevel;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.*;


public class TransportConnector implements Connector {
  private final ConnectorMetadata connectorMetadata;
  private final FunctionProvider functionProvider;

  public TransportConnector(ConnectorMetadata connectorMetadata, FunctionProvider functionProvider) {
    this.connectorMetadata = requireNonNull(connectorMetadata, "connector metadata is null");
    this.functionProvider = requireNonNull(functionProvider, "function provider is null");
  }

  public TransportConnector(Map<FunctionId, StdUdfWrapper> functions) {
    this.connectorMetadata = new TransportConnectorMetadata(functions);
    this.functionProvider = new TransportFunctionProvider(functions);
  }

  @Override
  public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
    return this.connectorMetadata;
  }

  @Override
  public Optional<FunctionProvider> getFunctionProvider() {
    return Optional.of(this.functionProvider);
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly,
      boolean autoCommit) {
    return TransportTransactionHandle.INSTANCE;
  }
}
