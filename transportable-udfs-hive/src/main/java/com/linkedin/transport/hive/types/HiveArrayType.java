/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.api.types.ArrayType;
import com.linkedin.transport.hive.HiveConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;


public class HiveArrayType implements ArrayType {

  final ListObjectInspector _listObjectInspector;

  public HiveArrayType(ListObjectInspector listObjectInspector) {
    _listObjectInspector = listObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _listObjectInspector;
  }

  @Override
  public DataType elementType() {
    return HiveConverters.toTransportType(_listObjectInspector.getListElementObjectInspector());
  }
}
