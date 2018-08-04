package com.linkedin.stdudfs.hive.data;

import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.hive.HiveWrapper;
import java.util.Iterator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;


public class HiveArray extends HiveData implements StdArray {

  final ListObjectInspector _listObjectInspector;
  final ObjectInspector _elementObjectInspector;

  public HiveArray(Object object, ObjectInspector objectInspector, StdFactory stdFactory) {
    super(stdFactory);
    _object = object;
    _listObjectInspector = (ListObjectInspector) objectInspector;
    _elementObjectInspector = _listObjectInspector.getListElementObjectInspector();
  }

  @Override
  public int size() {
    return _listObjectInspector.getListLength(_object);
  }

  @Override
  public StdData get(int idx) {
    return HiveWrapper.createStdData(_listObjectInspector.getListElement(_object, idx), _elementObjectInspector,
        _stdFactory);
  }

  @Override
  public void add(StdData e) {
    if (_listObjectInspector instanceof SettableListObjectInspector) {
      SettableListObjectInspector settableListObjectInspector = (SettableListObjectInspector) _listObjectInspector;
      int originalSize = size();
      settableListObjectInspector.resize(_object, originalSize + 1);
      settableListObjectInspector.set(_object, originalSize,
          ((HiveData) e).getUnderlyingDataForObjectInspector(_elementObjectInspector));
      _isObjectModified = true;
    } else {
      throw new RuntimeException("Attempt to modify an immutable Hive object of type: "
          + _listObjectInspector.getClass());
    }
  }

  @Override
  public ObjectInspector getUnderlyingObjectInspector() {
    return _listObjectInspector;
  }

  @Override
  public Iterator<StdData> iterator() {
    return new Iterator<StdData>() {
      int size = size();
      int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex != size;
      }

      @Override
      public StdData next() {
        StdData element = HiveWrapper.createStdData(_listObjectInspector.getListElement(_object, currentIndex),
            _elementObjectInspector, _stdFactory);
        currentIndex++;
        return element;
      }
    };
  }
}
