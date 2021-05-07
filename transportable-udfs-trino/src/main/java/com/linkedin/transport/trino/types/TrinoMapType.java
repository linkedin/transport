/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StdMapType;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.trino.TrinoWrapper;
import io.trino.spi.type.MapType;


public class TrinoMapType implements StdMapType {

  final MapType mapType;

  public TrinoMapType(MapType mapType) {
    this.mapType = mapType;
  }

  @Override
  public StdType keyType() {
    return TrinoWrapper.createStdType(mapType.getKeyType());
  }

  @Override
  public StdType valueType() {
    return TrinoWrapper.createStdType(mapType.getKeyType());
  }

  @Override
  public Object underlyingType() {
    return mapType;
  }
}
