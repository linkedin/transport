/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdLong;
import com.linkedin.transport.api.udf.StdUDF2;
import java.util.List;


public class NumericAddLongFunction extends StdUDF2<StdLong, StdLong, StdLong> implements NumericAddFunction {
  @Override
  public StdLong eval(StdLong first, StdLong second) {
    return getStdFactory().createLong(first.get() + second.get());
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of("bigint", "bigint");
  }

  @Override
  public String getOutputParameterSignature() {
    return "bigint";
  }
}
