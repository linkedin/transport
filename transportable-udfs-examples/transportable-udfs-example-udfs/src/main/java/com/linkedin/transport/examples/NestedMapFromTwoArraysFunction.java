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
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.api.udf.StdUDF1;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class NestedMapFromTwoArraysFunction extends StdUDF1<ArrayData, ArrayData> implements TopLevelStdUDF {

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
  public ArrayData eval(ArrayData a1) {
    ArrayData result = getStdFactory().createArray(_arrayType);

    for (int i = 0; i < a1.size(); i++) {
      if (a1.get(i) == null) {
        return null;
      }
      RowData inputRow = (RowData) a1.get(i);

      if (inputRow.getField(0) == null || inputRow.getField(1) == null) {
        return null;
      }
      ArrayData kValues = (ArrayData) inputRow.getField(0);
      ArrayData vValues = (ArrayData) inputRow.getField(1);

      if (kValues.size() != vValues.size()) {
        return null;
      }

      MapData map = getStdFactory().createMap(_mapType);
      for (int j = 0; j < kValues.size(); j++) {
        map.put(kValues.get(j), vValues.get(j));
      }

      RowData outputRow = getStdFactory().createStruct(_rowType);
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
