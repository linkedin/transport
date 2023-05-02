/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.TypeDeserializerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Map;

import static io.trino.plugin.base.Versions.*;
import static java.util.Objects.*;

/**
 * This class implements the interface of ConnectorFactory from Trino SPI as a part of Trino plugin
 * to load UDF classes in Trino server following the development guideline
 * in https://trino.io/docs/current/develop/spi-overview.html
 */
public class TransportConnectorFactory implements ConnectorFactory {
  @Override
  public String getName() {
    return "transport";
  }

  @Override
  public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
    requireNonNull(config, "config  is null");
    checkSpiVersion(context, this);

    // A plugin is not required to use Guice; it is just very convenient
    Bootstrap app = new Bootstrap(
        new TypeDeserializerModule(context.getTypeManager()),
        new TransportModule());

    Injector injector = app
        .doNotInitializeLogging()
        .setRequiredConfigurationProperties(config)
        .initialize();

    return injector.getInstance(TransportConnector.class);
  }
}
