/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.data;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdFloat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;


public class HiveFloat extends HiveData implements StdFloat {
  final FloatObjectInspector _floatObjectInspector;

  public HiveFloat(Object object, FloatObjectInspector floatObjectInspector, StdFactory stdFactory) {
    super(stdFactory);
    _object = object;
    _floatObjectInspector = floatObjectInspector;
  }

  @Override
  public float get() {
    return _floatObjectInspector.get(_object);
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _floatObjectInspector;
  }
}
