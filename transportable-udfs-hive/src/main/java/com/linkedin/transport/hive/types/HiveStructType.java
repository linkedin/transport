/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.StdStructType;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.hive.HiveWrapper;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;


public class HiveStructType implements StdStructType {

  final StructObjectInspector _structObjectInspector;

  public HiveStructType(StructObjectInspector structObjectInspector) {
    _structObjectInspector = structObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _structObjectInspector;
  }

  @Override
  public List<String> fieldNames() {
    return _structObjectInspector.getAllStructFieldRefs().stream()
        .map(StructField::getFieldName).collect(Collectors.toList());
  }


  @Override
  public List<? extends StdType> fieldTypes() {
    return _structObjectInspector.getAllStructFieldRefs().stream()
        .map(f -> HiveWrapper.createStdType(f.getFieldObjectInspector())).collect(Collectors.toList());
  }
}
