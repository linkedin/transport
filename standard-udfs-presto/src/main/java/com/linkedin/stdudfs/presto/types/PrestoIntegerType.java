/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.spi.type.IntegerType;
import com.linkedin.stdudfs.api.types.StdIntegerType;


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
