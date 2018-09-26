/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.hive.data;

import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdInteger;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;


public class HiveInteger extends HiveData implements StdInteger {

  final IntObjectInspector _intObjectInspector;

  public HiveInteger(Object object, IntObjectInspector intObjectInspector, StdFactory stdFactory) {
    super(stdFactory);
    _object = object;
    _intObjectInspector = intObjectInspector;
  }

  @Override
  public int get() {
    return _intObjectInspector.get(_object);
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _intObjectInspector;
  }
}
