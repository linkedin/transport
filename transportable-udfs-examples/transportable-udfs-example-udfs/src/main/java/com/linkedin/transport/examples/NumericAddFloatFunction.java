/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.StdUDF2;
import java.util.List;


public class NumericAddFloatFunction extends StdUDF2<Float, Float, Float> implements NumericAddFunction {
  @Override
  public Float eval(Float first, Float second) {
    return first + second;
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of("real", "real");
  }

  @Override
  public String getOutputParameterSignature() {
    return "real";
  }
}
