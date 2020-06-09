/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.StdFloatType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;


public class HiveFloatType implements StdFloatType {
  final FloatObjectInspector _floatObjectInspector;

  public HiveFloatType(FloatObjectInspector floatObjectInspector) {
    _floatObjectInspector = floatObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _floatObjectInspector;
  }
}
