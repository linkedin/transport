/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.api.udf.StdUDF2;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class MapFromTwoArraysFunction<K, V> extends StdUDF2<ArrayData<K>, ArrayData<V>, MapData<K, V>>
    implements TopLevelStdUDF {

  private StdType _mapType;

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
  public void init(StdFactory stdFactory) {
    super.init(stdFactory);
    // Note: we create the _mapType once in init() and then reuse it to create MapData objects
    _mapType = getStdFactory().createStdType(getOutputParameterSignature());
  }

  @Override
  public MapData<K, V> eval(ArrayData<K> a1, ArrayData<V> a2) {
    if (a1.size() != a2.size()) {
      return null;
    }
    MapData<K, V> map = getStdFactory().createMap(_mapType);
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
