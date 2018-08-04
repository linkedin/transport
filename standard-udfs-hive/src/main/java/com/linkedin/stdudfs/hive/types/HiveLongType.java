package com.linkedin.stdudfs.hive.types;

import com.linkedin.stdudfs.api.types.StdLongType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;


public class HiveLongType implements StdLongType {

  final LongObjectInspector _longObjectInspector;

  public HiveLongType(LongObjectInspector longObjectInspector) {
    _longObjectInspector = longObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _longObjectInspector;
  }
}
