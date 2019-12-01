/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.types;

import com.linkedin.transport.api.types.StdBooleanType;
import io.prestosql.spi.type.BooleanType;


public class PrestoBooleanType implements StdBooleanType {

  final BooleanType booleanType;

  public PrestoBooleanType(BooleanType booleanType) {
    this.booleanType = booleanType;
  }

  @Override
  public Object underlyingType() {
    return booleanType;
  }
}
