/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.StdDoubleType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;


public class HiveDoubleType implements StdDoubleType {

  private final DoubleObjectInspector _doubleObjectInspector;

  public HiveDoubleType(DoubleObjectInspector doubleObjectInspector) {
    _doubleObjectInspector = doubleObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _doubleObjectInspector;
  }
}
