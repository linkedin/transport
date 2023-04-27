/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.Plugin;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;


public class TransportPluginTest {

  @Test
  public void testTransportPluginInitialization() {
    TestingTrinoServer server = TestingTrinoServer.create();
    Plugin plugin = new TransportPlugin();
    server.installPlugin(plugin);
    server.createCatalog("LINKEDIN", "TRANSPORT");
    Assert.assertTrue(getOnlyElement(plugin.getConnectorFactories()) instanceof TransportConnectorFactory);
  }
}
