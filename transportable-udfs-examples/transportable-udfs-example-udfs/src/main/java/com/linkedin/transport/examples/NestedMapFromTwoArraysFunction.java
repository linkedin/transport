/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdMap;
import com.linkedin.transport.api.data.StdStruct;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.api.udf.StdUDF1;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class NestedMapFromTwoArraysFunction extends StdUDF1<StdArray, StdArray> implements TopLevelStdUDF {

  private StdType _arrayType;
  private StdType _mapType;
  private StdType _rowType;

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "array(row(array(K),array(V)))"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "array(row(map(K,V)))";
  }

  @Override
  public void init(StdFactory stdFactory) {
    super.init(stdFactory);
    _arrayType = getStdFactory().createStdType(getOutputParameterSignature());
    _rowType = getStdFactory().createStdType("row(map(K,V))");
    _mapType = getStdFactory().createStdType("map(K,V)");
  }

  @Override
  public StdArray eval(StdArray a1) {
    StdArray result = getStdFactory().createArray(_arrayType);

    for (int i = 0; i < a1.size(); i++) {
      StdStruct inputRow = (StdStruct) a1.get(i);
      StdArray kValues = (StdArray) inputRow.getField(0);
      StdArray vValues = (StdArray) inputRow.getField(1);

      StdMap map = getStdFactory().createMap(_mapType);
      for (int j = 0; j < kValues.size(); j++) {
        map.put(kValues.get(j), vValues.get(j));
      }

      StdStruct outputRow = getStdFactory().createStruct(_rowType);
      outputRow.setField(0, map);

      result.add(outputRow);
    }

    return result;
  }

  @Override
  public String getFunctionName() {
    return "nested_map_from_two_arrays";
  }

  @Override
  public String getFunctionDescription() {
    return "Create a nested map from the 2 nested arrays";
  }
}
