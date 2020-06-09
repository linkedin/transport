/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.types;

import com.linkedin.transport.api.types.StdLongType;
import io.prestosql.spi.type.BigintType;


public class PrestoLongType implements StdLongType {

  final BigintType bigintType;

  public PrestoLongType(BigintType bigintType) {
    this.bigintType = bigintType;
  }

  @Override
  public Object underlyingType() {
    return bigintType;
  }
}
