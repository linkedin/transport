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
