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
import com.linkedin.transport.api.udf.UDF2;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.util.List;


public class MapFromTwoArraysFunction<K, V> extends UDF2<ArrayData<K>, ArrayData<V>, MapData<K, V>>
    implements TopLevelUDF {

  private DataType _mapType;

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "array(K)",
        "array(V)"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "map(K,V)";
  }

  @Override
  public void init(TypeFactory typeFactory) {
    super.init(typeFactory);
    // Note: we create the _mapType once in init() and then reuse it to create MapData objects
    _mapType = getTypeFactory().createDataType(getOutputParameterSignature());
  }

  @Override
  public MapData<K, V> eval(ArrayData<K> a1, ArrayData<V> a2) {
    if (a1.size() != a2.size()) {
      return null;
    }
    MapData<K, V> map = getTypeFactory().createMap(_mapType);
    for (int i = 0; i < a1.size(); i++) {
      map.put(a1.get(i), a2.get(i));
    }
    return map;
  }

  @Override
  public String getFunctionName() {
    return "map_from_two_arrays";
  }

  @Override
  public String getFunctionDescription() {
    return "Create a map out of two arrays.";
  }
}
