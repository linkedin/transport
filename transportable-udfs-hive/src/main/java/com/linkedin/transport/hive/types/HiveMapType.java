/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.types;

import com.linkedin.transport.api.types.MapType;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.hive.HiveWrapper;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;


public class HiveMapType implements MapType {

  final MapObjectInspector _mapObjectInspector;

  public HiveMapType(MapObjectInspector mapObjectInspector) {
    _mapObjectInspector = mapObjectInspector;
  }

  @Override
  public Object underlyingType() {
    return _mapObjectInspector;
  }

  @Override
  public DataType keyType() {
    return HiveWrapper.createStdType(_mapObjectInspector.getMapKeyObjectInspector());
  }

  @Override
  public DataType valueType() {
    return HiveWrapper.createStdType(_mapObjectInspector.getMapValueObjectInspector());
  }
}
