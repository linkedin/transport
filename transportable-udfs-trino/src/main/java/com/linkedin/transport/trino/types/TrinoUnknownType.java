/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.UnknownType;


public class TrinoUnknownType implements UnknownType {

  final io.trino.type.UnknownType unknownType;

  public TrinoUnknownType(io.trino.type.UnknownType unknownType) {
    this.unknownType = unknownType;
  }

  @Override
  public Object underlyingType() {
    return unknownType;
  }
}
