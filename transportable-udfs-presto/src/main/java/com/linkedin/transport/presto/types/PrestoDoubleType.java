/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.types;

import com.linkedin.transport.api.types.StdDoubleType;
import io.prestosql.spi.type.DoubleType;


public class PrestoDoubleType implements StdDoubleType {

  final DoubleType doubleType;

  public PrestoDoubleType(DoubleType doubleType) {
    this.doubleType = doubleType;
  }

  @Override
  public Object underlyingType() {
    return doubleType;
  }
}
