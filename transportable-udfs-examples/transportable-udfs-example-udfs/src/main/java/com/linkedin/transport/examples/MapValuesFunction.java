/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.api.udf.UDF1;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.util.List;


public class MapValuesFunction<K, V> extends UDF1<MapData<K, V>, ArrayData<V>> implements TopLevelUDF {

  private DataType _mapType;

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "map(K,V)"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "array(V)";
  }

  @Override
  public void init(TypeFactory typeFactory) {
    super.init(typeFactory);
    _mapType = getTypeFactory().createDataType(getOutputParameterSignature());
  }

  @Override
  public ArrayData<V> eval(MapData<K, V> map) {
    ArrayData<V> result = getTypeFactory().createArray(_mapType);
    for (V value : map.values()) {
      result.add(value);
    }
    return result;
  }

  @Override
  public String getFunctionName() {
    return "transport_map_values";
  }

  @Override
  public String getFunctionDescription() {
    return "Returns the set of values from a map";
  }
}
