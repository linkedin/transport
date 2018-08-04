package com.linkedin.stdudfs.hive.types;

import com.linkedin.stdudfs.api.types.StdBooleanType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;


public class HiveBooleanType implements StdBooleanType {

  final BooleanObjectInspector _booleanObjectInspector;

  public HiveBooleanType(BooleanObjectInspector booleanObjectInspector) {
    _booleanObjectInspector = booleanObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _booleanObjectInspector;
  }
}
