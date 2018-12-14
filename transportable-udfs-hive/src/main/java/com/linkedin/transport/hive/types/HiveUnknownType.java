/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.StdUnknownType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;


public class HiveUnknownType implements StdUnknownType {

  private VoidObjectInspector _voidObjectInspector;

  public HiveUnknownType(VoidObjectInspector voidObjectInspector) {
    _voidObjectInspector = voidObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _voidObjectInspector;
  }
}
