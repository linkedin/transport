/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StringType;
import io.trino.spi.type.VarcharType;


public class TrinoStringType implements StringType {

  final VarcharType varcharType;

  public TrinoStringType(VarcharType varcharType) {
    this.varcharType = varcharType;
  }

  @Override
  public Object underlyingType() {
    return varcharType;
  }
}
