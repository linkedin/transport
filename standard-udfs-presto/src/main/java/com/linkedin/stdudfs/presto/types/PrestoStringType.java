/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.spi.type.VarcharType;
import com.linkedin.stdudfs.api.types.StdStringType;


public class PrestoStringType implements StdStringType {

  final VarcharType varcharType;

  public PrestoStringType(VarcharType varcharType) {
    this.varcharType = varcharType;
  }

  @Override
  public Object underlyingType() {
    return varcharType;
  }
}
