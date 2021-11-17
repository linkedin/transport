/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.RowType;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.hive.HiveWrapper;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;


public class HiveRowType implements RowType {

  final StructObjectInspector _structObjectInspector;

  public HiveRowType(StructObjectInspector structObjectInspector) {
    _structObjectInspector = structObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _structObjectInspector;
  }

  @Override
  public List<? extends DataType> fieldTypes() {
    return _structObjectInspector.getAllStructFieldRefs().stream()
        .map(f -> HiveWrapper.createStdType(f.getFieldObjectInspector())).collect(Collectors.toList());
  }
}
