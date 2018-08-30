/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
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
