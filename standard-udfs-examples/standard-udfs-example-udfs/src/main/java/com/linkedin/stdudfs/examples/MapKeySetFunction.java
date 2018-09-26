/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.data.StdMap;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.api.udf.StdUDF1;
import com.linkedin.stdudfs.api.udf.TopLevelStdUDF;
import java.util.List;


public class MapKeySetFunction extends StdUDF1<StdMap, StdArray> implements TopLevelStdUDF {

  private StdType _mapType;

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
  public void init(StdFactory stdFactory) {
    super.init(stdFactory);
    _mapType = getStdFactory().createStdType(getOutputParameterSignature());
  }

  @Override
  public StdArray eval(StdMap map) {
    StdArray result = getStdFactory().createArray(_mapType);
    for (StdData key : map.keySet()) {
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
