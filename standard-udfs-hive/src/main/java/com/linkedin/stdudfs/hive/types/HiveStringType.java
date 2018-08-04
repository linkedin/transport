package com.linkedin.stdudfs.hive.types;

import com.linkedin.stdudfs.api.types.StdStringType;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;


public class HiveStringType implements StdStringType {

  final StringObjectInspector _stringObjectInspector;

  public HiveStringType(StringObjectInspector stringObjectInspector) {
    _stringObjectInspector = stringObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _stringObjectInspector;
  }
}
