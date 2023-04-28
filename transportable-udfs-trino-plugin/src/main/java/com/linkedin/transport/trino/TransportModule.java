/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.*;

/**
 * This class implements the interface of Module from Trino SPI as a part of Trino plugin
 * to load UDF classes in Trino server following the development guideline
 * in https://trino.io/docs/current/develop/spi-overview.html
 */
public class TransportModule implements Module {
  @Override
  public void configure(Binder binder) {
    binder.bind(TransportConnector.class).in(Scopes.SINGLETON);
    configBinder(binder).bindConfig(TransportConfig.class);
  }
}
