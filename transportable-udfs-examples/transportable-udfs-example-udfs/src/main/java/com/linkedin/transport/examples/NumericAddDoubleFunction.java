/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.UDF2;
import java.util.List;


public class NumericAddDoubleFunction extends UDF2<Double, Double, Double> implements NumericAddFunction {
  @Override
  public Double eval(Double first, Double second) {
    return first + second;
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of("double", "double");
  }

  @Override
  public String getOutputParameterSignature() {
    return "double";
  }
}
