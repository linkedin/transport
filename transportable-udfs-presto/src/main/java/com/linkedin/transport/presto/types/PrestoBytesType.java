/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.types;

import com.linkedin.transport.api.types.StdDoubleType;
import io.prestosql.spi.type.VarbinaryType;


public class PrestoBytesType implements StdDoubleType {

  final VarbinaryType varbinaryType;

  public PrestoBytesType(VarbinaryType varbinaryType) {
    this.varbinaryType = varbinaryType;
  }

  @Override
  public Object underlyingType() {
    return varbinaryType;
  }
}
