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


public class TransportModule implements Module {
  @Override
  public void configure(Binder binder) {
    binder.bind(TransportConnector.class).in(Scopes.SINGLETON);
    configBinder(binder).bindConfig(TransportConfig.class);
  }
}
