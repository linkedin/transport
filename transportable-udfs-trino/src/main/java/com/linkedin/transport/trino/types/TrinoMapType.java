/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.api.types.MapType;
import com.linkedin.transport.trino.TrinoWrapper;


public class TrinoMapType implements MapType {

  final io.trino.spi.type.MapType mapType;

  public TrinoMapType(io.trino.spi.type.MapType mapType) {
    this.mapType = mapType;
  }

  @Override
  public DataType keyType() {
    return TrinoWrapper.createStdType(mapType.getKeyType());
  }

  @Override
  public DataType valueType() {
    return TrinoWrapper.createStdType(mapType.getKeyType());
  }

  @Override
  public Object underlyingType() {
    return mapType;
  }
}
