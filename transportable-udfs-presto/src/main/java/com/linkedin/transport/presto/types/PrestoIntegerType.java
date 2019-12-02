/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.types;

import com.linkedin.transport.api.types.StdIntegerType;
import io.prestosql.spi.type.IntegerType;


public class PrestoIntegerType implements StdIntegerType {

  final IntegerType integerType;

  public PrestoIntegerType(IntegerType integerType) {
    this.integerType = integerType;
  }

  @Override
  public Object underlyingType() {
    return integerType;
  }
}
