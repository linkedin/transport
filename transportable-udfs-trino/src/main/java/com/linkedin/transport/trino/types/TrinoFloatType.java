/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StdFloatType;
import io.trino.spi.type.RealType;


public class TrinoFloatType implements StdFloatType {

  private final RealType floatType;

  public TrinoFloatType(RealType floatType) {
    this.floatType = floatType;
  }

  @Override
  public Object underlyingType() {
    return floatType;
  }
}
