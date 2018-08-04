package com.linkedin.stdudfs.hive.data;

import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.data.StdStruct;
import com.linkedin.stdudfs.hive.HiveWrapper;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;


public class HiveStruct extends HiveData implements StdStruct {

  StructObjectInspector _structObjectInspector;

  public HiveStruct(Object object, ObjectInspector objectInspector, StdFactory stdFactory) {
    super(stdFactory);
    _object = object;
    _structObjectInspector = (StructObjectInspector) objectInspector;
  }

  @Override
  public StdData getField(int index) {
    StructField structField = _structObjectInspector.getAllStructFieldRefs().get(index);
    return HiveWrapper.createStdData(
        _structObjectInspector.getStructFieldData(_object, structField),
        structField.getFieldObjectInspector(), _stdFactory
    );
  }

  @Override
  public StdData getField(String name) {
    StructField structField = _structObjectInspector.getStructFieldRef(name);
    return HiveWrapper.createStdData(
        _structObjectInspector.getStructFieldData(_object, structField),
        structField.getFieldObjectInspector(), _stdFactory
    );
  }

  @Override
  public void setField(int index, StdData value) {
    if (_structObjectInspector instanceof SettableStructObjectInspector) {
      StructField field = _structObjectInspector.getAllStructFieldRefs().get(index);
      ((SettableStructObjectInspector) _structObjectInspector).setStructFieldData(_object,
          field, ((HiveData) value).getUnderlyingDataForObjectInspector(field.getFieldObjectInspector())
      );
      _isObjectModified = true;
    } else {
      throw new RuntimeException("Attempt to modify an immutable Hive object of type: "
          + _structObjectInspector.getClass());
    }
  }

  @Override
  public void setField(String name, StdData value) {
    if (_structObjectInspector instanceof SettableStructObjectInspector) {
      StructField field = _structObjectInspector.getStructFieldRef(name);
      ((SettableStructObjectInspector) _structObjectInspector).setStructFieldData(_object,
          field, ((HiveData) value).getUnderlyingDataForObjectInspector(field.getFieldObjectInspector()));
      _isObjectModified = true;
    } else {
      throw new RuntimeException("Attempt to modify an immutable Hive object of type: "
          + _structObjectInspector.getClass());
    }
  }

  @Override
  public List<StdData> fields() {
    return IntStream.range(0, _structObjectInspector.getAllStructFieldRefs().size()).mapToObj(i -> getField(i))
        .collect(Collectors.toList());
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _structObjectInspector;
  }
}
