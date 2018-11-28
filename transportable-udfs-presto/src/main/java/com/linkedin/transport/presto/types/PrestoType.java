/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto.types;

import com.facebook.presto.spi.type.Type;
import com.linkedin.transport.api.types.StdType;


public class PrestoType implements StdType {

  final Type type;

  public PrestoType(Type type) {
    this.type = type;
  }

  @Override
  public Object underlyingType() {
    return type;
  }
}
