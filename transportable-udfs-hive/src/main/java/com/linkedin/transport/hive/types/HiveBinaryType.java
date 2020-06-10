/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.StdBinaryType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;


public class HiveBinaryType implements StdBinaryType {

  private final BinaryObjectInspector _binaryObjectInspector;

  public HiveBinaryType(BinaryObjectInspector binaryObjectInspector) {
    _binaryObjectInspector = binaryObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _binaryObjectInspector;
  }
}
