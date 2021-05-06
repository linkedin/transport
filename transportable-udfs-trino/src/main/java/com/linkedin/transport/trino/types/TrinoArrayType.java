/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StdArrayType;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.trino.TrinoWrapper;
import io.trino.spi.type.ArrayType;


public class TrinoArrayType implements StdArrayType {

  final ArrayType arrayType;

  public TrinoArrayType(ArrayType arrayType) {
    this.arrayType = arrayType;
  }

  @Override
  public StdType elementType() {
    return TrinoWrapper.createStdType(arrayType.getElementType());
  }

  @Override
  public Object underlyingType() {
    return arrayType;
  }
}
