/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.StringType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;


public class HiveStringType implements StringType {

  final StringObjectInspector _stringObjectInspector;

  public HiveStringType(StringObjectInspector stringObjectInspector) {
    _stringObjectInspector = stringObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _stringObjectInspector;
  }
}
