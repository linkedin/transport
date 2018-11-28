/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.data;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdLong;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;


public class HiveLong extends HiveData implements StdLong {

  final LongObjectInspector _longObjectInspector;

  public HiveLong(Object object, LongObjectInspector longObjectInspector, StdFactory stdFactory) {
    super(stdFactory);
    _object = object;
    _longObjectInspector = longObjectInspector;
  }

  @Override
  public long get() {
    return _longObjectInspector.get(_object);
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _longObjectInspector;
  }
}
