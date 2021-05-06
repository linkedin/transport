/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StdDoubleType;
import io.trino.spi.type.DoubleType;


public class TrinoDoubleType implements StdDoubleType {

  private final DoubleType doubleType;

  public TrinoDoubleType(DoubleType doubleType) {
    this.doubleType = doubleType;
  }

  @Override
  public Object underlyingType() {
    return doubleType;
  }
}
