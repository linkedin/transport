/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StdBinaryType;
import io.trino.spi.type.VarbinaryType;


public class TrinoBinaryType implements StdBinaryType {

  private final VarbinaryType varbinaryType;

  public TrinoBinaryType(VarbinaryType varbinaryType) {
    this.varbinaryType = varbinaryType;
  }

  @Override
  public Object underlyingType() {
    return varbinaryType;
  }
}
