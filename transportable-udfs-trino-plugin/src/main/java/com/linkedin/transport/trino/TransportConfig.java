/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import io.airlift.configuration.Config;


/**
 * This class defines the configuration which is used by Trino plugin to load UDF classes in Trino server
 * following the development guideline in https://trino.io/docs/current/develop/spi-overview.html
 */
public class TransportConfig {
  private String transportUdfRepo;

  public String getTransportUdfRepo() {
    return transportUdfRepo;
  }

  @Config("transport.udf.repo")
  public TransportConfig setTransportUdfRepo(String transportUdfRepo) {
    this.transportUdfRepo = transportUdfRepo;
    return this;
  }
}
