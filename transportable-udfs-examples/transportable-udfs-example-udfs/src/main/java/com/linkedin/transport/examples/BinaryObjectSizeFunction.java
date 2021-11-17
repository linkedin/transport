/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.udf.UDF1;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.nio.ByteBuffer;
import java.util.List;


public class BinaryObjectSizeFunction extends UDF1<ByteBuffer, Integer> implements TopLevelUDF {
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
