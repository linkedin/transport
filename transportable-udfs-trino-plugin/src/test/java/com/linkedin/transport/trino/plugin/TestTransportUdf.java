/**
 * Copyright 2019 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.plugin;

import com.linkedin.transport.trino.TransportPlugin;
import io.trino.server.testing.TestingTrinoServer;
import java.sql.Statement;
import org.testng.annotations.BeforeClass;


public class TestTransportUdf {
  private TestingTrinoServer server;
  private Statement statement;

  @BeforeClass
  public void setupServer() throws Exception {
    server = TestingTrinoServer.create();
    server.installPlugin(new TransportPlugin());
    server.createCatalog("LINKEDIN", "TRANSPORT");
  }
}
