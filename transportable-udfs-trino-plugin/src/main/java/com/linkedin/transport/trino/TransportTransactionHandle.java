/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 * This class implements the interface of ConnectorTransactionHandle from Trino SPI as a part of Trino plugin
 * to load UDF classes in Trino server following the development guideline
 * in https://trino.io/docs/current/develop/spi-overview.html
 */
public enum TransportTransactionHandle implements ConnectorTransactionHandle {
  INSTANCE
}