/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;


public class TransportPlugin implements Plugin {
  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    return ImmutableList.of(new TransportConnectorFactory());
  }
}
