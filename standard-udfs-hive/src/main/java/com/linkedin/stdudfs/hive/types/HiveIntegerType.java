/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.hive.types;

import com.linkedin.stdudfs.api.types.StdIntegerType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;


public class HiveIntegerType implements StdIntegerType {

  final IntObjectInspector _intObjectInspector;

  public HiveIntegerType(IntObjectInspector intObjectInspector) {
    _intObjectInspector = intObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _intObjectInspector;
  }
}
