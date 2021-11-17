/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.data;

import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.hive.HiveWrapper;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;


public class HiveRowData extends HiveData implements RowData {

  StructObjectInspector _structObjectInspector;

  public HiveRowData(Object object, ObjectInspector objectInspector, TypeFactory typeFactory) {
    super(typeFactory);
    _object = object;
    _structObjectInspector = (StructObjectInspector) objectInspector;
  }

  @Override
  public Object getField(int index) {
    StructField structField = _structObjectInspector.getAllStructFieldRefs().get(index);
    return HiveWrapper.createStdData(
        _structObjectInspector.getStructFieldData(_object, structField),
        structField.getFieldObjectInspector(), _typeFactory
    );
  }

  @Override
  public Object getField(String name) {
    StructField structField = _structObjectInspector.getStructFieldRef(name);
    return HiveWrapper.createStdData(
        _structObjectInspector.getStructFieldData(_object, structField),
        structField.getFieldObjectInspector(), _typeFactory
    );
  }

  @Override
  public void setField(int index, Object value) {
    if (_structObjectInspector instanceof SettableStructObjectInspector) {
      StructField field = _structObjectInspector.getAllStructFieldRefs().get(index);
      ((SettableStructObjectInspector) _structObjectInspector).setStructFieldData(_object,
          field, HiveWrapper.getPlatformDataForObjectInspector(value, field.getFieldObjectInspector())
      );
      _isObjectModified = true;
    } else {
      throw new RuntimeException("Attempt to modify an immutable Hive object of type: "
          + _structObjectInspector.getClass());
    }
  }

  @Override
  public void setField(String name, Object value) {
    if (_structObjectInspector instanceof SettableStructObjectInspector) {
      StructField field = _structObjectInspector.getStructFieldRef(name);
      ((SettableStructObjectInspector) _structObjectInspector).setStructFieldData(_object,
          field, HiveWrapper.getPlatformDataForObjectInspector(value, field.getFieldObjectInspector()));
      _isObjectModified = true;
    } else {
      throw new RuntimeException("Attempt to modify an immutable Hive object of type: "
          + _structObjectInspector.getClass());
    }
  }

  @Override
  public List<Object> fields() {
    return IntStream.range(0, _structObjectInspector.getAllStructFieldRefs().size()).mapToObj(i -> getField(i))
        .collect(Collectors.toList());
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _structObjectInspector;
  }
}
