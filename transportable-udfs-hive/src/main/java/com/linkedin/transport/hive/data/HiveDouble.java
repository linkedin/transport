/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.data;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdDouble;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;


public class HiveDouble extends HiveData implements StdDouble {

  private final DoubleObjectInspector _doubleObjectInspector;

  public HiveDouble(Object object, DoubleObjectInspector floatObjectInspector, StdFactory stdFactory) {
    super(stdFactory);
    _object = object;
    _doubleObjectInspector = floatObjectInspector;
  }

  @Override
  public double get() {
    return _doubleObjectInspector.get(_object);
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _doubleObjectInspector;
  }
}
