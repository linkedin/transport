/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdDouble;
import com.linkedin.transport.api.udf.StdUDF2;
import java.util.List;


public class NumericAddDoubleFunction extends StdUDF2<StdDouble, StdDouble, StdDouble> implements NumericAddFunction {
  @Override
  public StdDouble eval(StdDouble first, StdDouble second) {
    return getStdFactory().createDouble(first.get() + second.get());
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
