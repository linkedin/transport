/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.presto.types;

import com.facebook.presto.spi.type.ArrayType;
import com.linkedin.stdudfs.api.types.StdArrayType;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.presto.PrestoWrapper;


public class PrestoArrayType implements StdArrayType {

  final ArrayType arrayType;

  public PrestoArrayType(ArrayType arrayType) {
    this.arrayType = arrayType;
  }

  @Override
  public StdType elementType() {
    return PrestoWrapper.createStdType(arrayType.getElementType());
  }

  @Override
  public Object underlyingType() {
    return arrayType;
  }
}
