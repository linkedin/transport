/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.types;

import com.linkedin.transport.api.types.StdFloatType;
import io.prestosql.spi.type.RealType;


public class PrestoFloatType implements StdFloatType {

  final RealType floatType;

  public PrestoFloatType(RealType floatType) {
    this.floatType = floatType;
  }

  @Override
  public Object underlyingType() {
    return floatType;
  }
}
