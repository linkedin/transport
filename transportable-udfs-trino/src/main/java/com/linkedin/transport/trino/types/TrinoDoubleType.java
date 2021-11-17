/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.DoubleType;


public class TrinoDoubleType implements DoubleType {

  private final io.trino.spi.type.DoubleType doubleType;

  public TrinoDoubleType(io.trino.spi.type.DoubleType doubleType) {
    this.doubleType = doubleType;
  }

  @Override
  public Object underlyingType() {
    return doubleType;
  }
}
