/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.data.StdStruct;
import com.linkedin.transport.api.udf.StdUDF1;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.util.List;


public class StructElementIncrementByOneFunction extends StdUDF1<StdStruct, StdStruct> implements TopLevelStdUDF {

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "row(integer, integer)"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "row(integer, integer)";
  }

  @Override
  public StdStruct eval(StdStruct myStruct) {
    int currVal = ((StdInteger) myStruct.getField(0)).get();
    myStruct.setField(0, getStdFactory().createInteger(currVal + 1));
    return myStruct;
  }

  @Override
  public String getFunctionName() {
    return "struct_element_increment_by_one";
  }

  @Override
  public String getFunctionDescription() {
    return "increment first element by one";
  }
}
