/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.data;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdBoolean;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;


public class HiveBoolean extends HiveData implements StdBoolean {

  final BooleanObjectInspector _booleanObjectInspector;

  public HiveBoolean(Object object, BooleanObjectInspector booleanObjectInspector, StdFactory stdFactory) {
    super(stdFactory);
    _object = object;
    _booleanObjectInspector = booleanObjectInspector;
  }

  @Override
  public boolean get() {
    return _booleanObjectInspector.get(_object);
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _booleanObjectInspector;
  }
}
