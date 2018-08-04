package com.linkedin.stdudfs.hive.data;

import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;


public class HiveString extends HiveData implements StdString {

  final StringObjectInspector _stringObjectInspector;

  public HiveString(Object object, StringObjectInspector stringObjectInspector, StdFactory stdFactory) {
    super(stdFactory);
    _object = object;
    _stringObjectInspector = stringObjectInspector;
  }

  @Override
  public String get() {
    return _stringObjectInspector.getPrimitiveJavaObject(_object);
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _stringObjectInspector;
  }
}
