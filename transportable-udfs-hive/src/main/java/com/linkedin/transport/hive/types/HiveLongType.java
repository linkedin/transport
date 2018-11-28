/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.StdLongType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;


public class HiveLongType implements StdLongType {

  final LongObjectInspector _longObjectInspector;

  public HiveLongType(LongObjectInspector longObjectInspector) {
    _longObjectInspector = longObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _longObjectInspector;
  }
}
