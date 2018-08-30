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
