/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.data.StdLong;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.api.udf.StdUDF2;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class ArrayFillFunction extends StdUDF2<StdData, StdLong, StdArray> implements TopLevelStdUDF {

  private StdType _arrayType;

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "K",
        "bigint"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "array(K)";
  }

  @Override
  public void init(StdFactory stdFactory) {
    super.init(stdFactory);
    _arrayType = getStdFactory().createStdType(getOutputParameterSignature());
  }

  @Override
  public StdArray eval(StdData a, StdLong length) {
    StdArray array = getStdFactory().createArray(_arrayType);
    for (int i = 0; i < length.get(); i++) {
      array.add(a);
    }
    return array;
  }

  @Override
  public String getFunctionName() {
    return "array_fill";
  }

  @Override
  public String getFunctionDescription() {
    return "Create an array given an element and a number of repetitions";
  }
}
