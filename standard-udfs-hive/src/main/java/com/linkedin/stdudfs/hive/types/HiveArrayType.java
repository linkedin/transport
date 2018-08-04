package com.linkedin.stdudfs.hive.types;

import com.linkedin.stdudfs.api.types.StdArrayType;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.hive.HiveWrapper;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;


public class HiveArrayType implements StdArrayType {

  final ListObjectInspector _listObjectInspector;

  public HiveArrayType(ListObjectInspector listObjectInspector) {
    _listObjectInspector = listObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _listObjectInspector;
  }

  @Override
  public StdType elementType() {
    return HiveWrapper.createStdType(_listObjectInspector.getListElementObjectInspector());
  }
}
