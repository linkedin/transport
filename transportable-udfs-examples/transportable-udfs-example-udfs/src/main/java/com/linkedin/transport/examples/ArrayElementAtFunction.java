/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.udf.StdUDF2;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class ArrayElementAtFunction extends StdUDF2<StdArray, StdInteger, StdData> implements TopLevelStdUDF {

  @Override
  public String getFunctionName() {
    return "array_element_at";
  }

  @Override
  public String getFunctionDescription() {
    return "Create a map out of two arrays.";
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "array(K)",
        "integer"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "K";
  }

  @Override
  public StdData eval(StdArray a1, StdInteger idx) {
    return a1.get(idx.get());
  }
}
