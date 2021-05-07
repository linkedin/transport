/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino.types;

import com.linkedin.transport.api.types.StdUnknownType;
import io.trino.type.UnknownType;


public class TrinoUnknownType implements StdUnknownType {

  final UnknownType unknownType;

  public TrinoUnknownType(UnknownType unknownType) {
    this.unknownType = unknownType;
  }

  @Override
  public Object underlyingType() {
    return unknownType;
  }
}
