/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.api.types.ArrayType;
import com.linkedin.transport.trino.TrinoConverters;


public class TrinoArrayType implements ArrayType {

  final io.trino.spi.type.ArrayType arrayType;

  public TrinoArrayType(io.trino.spi.type.ArrayType arrayType) {
    this.arrayType = arrayType;
  }

  @Override
  public DataType elementType() {
    return TrinoConverters.toTransportType(arrayType.getElementType());
  }

  @Override
  public Object underlyingType() {
    return arrayType;
  }
}
