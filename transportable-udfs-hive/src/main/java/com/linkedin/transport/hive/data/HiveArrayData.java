/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.data;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.hive.HiveWrapper;
import java.util.Iterator;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;


public class HiveArrayData<E> extends HiveData implements ArrayData<E> {

  final ListObjectInspector _listObjectInspector;
  final ObjectInspector _elementObjectInspector;

  public HiveArrayData(Object object, ObjectInspector objectInspector, StdFactory stdFactory) {
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
  public E get(int idx) {
    return (E) HiveWrapper.createStdData(
        _listObjectInspector.getListElement(_object, idx),
        _elementObjectInspector,
        _stdFactory);
  }

  @Override
  public void add(E e) {
    if (_listObjectInspector instanceof SettableListObjectInspector) {
      SettableListObjectInspector settableListObjectInspector = (SettableListObjectInspector) _listObjectInspector;
      int originalSize = size();
      settableListObjectInspector.resize(_object, originalSize + 1);
      settableListObjectInspector.set(_object, originalSize,
          HiveWrapper.getPlatformDataForObjectInspector(e, _elementObjectInspector));
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
  public Iterator<E> iterator() {
    return new Iterator<E>() {
      int size = size();
      int currentIndex = 0;

      @Override
      public boolean hasNext() {
        return currentIndex != size;
      }

      @Override
      public E next() {
        E element = (E) HiveWrapper.createStdData(_listObjectInspector.getListElement(_object, currentIndex),
            _elementObjectInspector, _stdFactory);
        currentIndex++;
        return element;
      }
    };
  }
}
