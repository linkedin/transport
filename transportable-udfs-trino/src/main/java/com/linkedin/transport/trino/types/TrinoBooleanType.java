/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StdBooleanType;
import io.trino.spi.type.BooleanType;


public class TrinoBooleanType implements StdBooleanType {

  final BooleanType booleanType;

  public TrinoBooleanType(BooleanType booleanType) {
    this.booleanType = booleanType;
  }

  @Override
  public Object underlyingType() {
    return booleanType;
  }
}
