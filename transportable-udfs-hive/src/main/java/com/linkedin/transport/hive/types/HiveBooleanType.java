/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.BooleanType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;


public class HiveBooleanType implements BooleanType {

  final BooleanObjectInspector _booleanObjectInspector;

  public HiveBooleanType(BooleanObjectInspector booleanObjectInspector) {
    _booleanObjectInspector = booleanObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _booleanObjectInspector;
  }
}
