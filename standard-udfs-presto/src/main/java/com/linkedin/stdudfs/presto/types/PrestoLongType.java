/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.spi.type.BigintType;
import com.linkedin.stdudfs.api.types.StdIntegerType;


public class PrestoLongType implements StdIntegerType {

  final BigintType bigintType;

  public PrestoLongType(BigintType bigintType) {
    this.bigintType = bigintType;
  }

  @Override
  public Object underlyingType() {
    return bigintType;
  }
}
