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


public class MapKeySetFunction<K, V> extends UDF1<MapData<K, V>, ArrayData<K>> implements TopLevelUDF {

  private DataType _mapType;

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "map(K,V)"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "array(K)";
  }

  @Override
  public void init(TypeFactory typeFactory) {
    super.init(typeFactory);
    _mapType = getTypeFactory().createDataType(getOutputParameterSignature());
  }

  @Override
  public ArrayData<K> eval(MapData<K, V> map) {
    ArrayData<K> result = getTypeFactory().createArray(_mapType);
    for (K key : map.keySet()) {
      result.add(key);
    }
    return result;
  }

  @Override
  public String getFunctionName() {
    return "map_key_set";
  }

  @Override
  public String getFunctionDescription() {
    return "Returns the set of keys from a map";
  }
}
