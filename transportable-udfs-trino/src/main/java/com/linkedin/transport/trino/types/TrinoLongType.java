/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StdLongType;
import io.trino.spi.type.BigintType;


public class TrinoLongType implements StdLongType {

  final BigintType bigintType;

  public TrinoLongType(BigintType bigintType) {
    this.bigintType = bigintType;
  }

  @Override
  public Object underlyingType() {
    return bigintType;
  }
}
