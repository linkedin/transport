/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.StdUDF1;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import java.nio.ByteBuffer;
import java.util.List;


public class BinaryObjectSizeFunction extends StdUDF1<ByteBuffer, Integer> implements TopLevelStdUDF {
  @Override
  public Integer eval(ByteBuffer byteBuffer) {
    return byteBuffer.array().length;
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of("varbinary");
  }

  @Override
  public String getOutputParameterSignature() {
    return "integer";
  }

  @Override
  public String getFunctionName() {
    return "binary_size";
  }

  @Override
  public String getFunctionDescription() {
    return "Gets the size of a binary object";
  }
}
