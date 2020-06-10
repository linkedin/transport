/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.data;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdBytes;
import java.nio.ByteBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;


public class HiveBytes extends HiveData implements StdBytes {

  private final BinaryObjectInspector _binaryObjectInspector;

  public HiveBytes(Object object, BinaryObjectInspector binaryObjectInspector, StdFactory stdFactory) {
    super(stdFactory);
    _object = object;
    _binaryObjectInspector = binaryObjectInspector;
  }

  @Override
  public ByteBuffer get() {
    return ByteBuffer.wrap(_binaryObjectInspector.getPrimitiveJavaObject(_object));
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _binaryObjectInspector;
  }
}
