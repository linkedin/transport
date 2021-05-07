/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StdIntegerType;
import io.trino.spi.type.IntegerType;


public class TrinoIntegerType implements StdIntegerType {

  final IntegerType integerType;

  public TrinoIntegerType(IntegerType integerType) {
    this.integerType = integerType;
  }

  @Override
  public Object underlyingType() {
    return integerType;
  }
}
