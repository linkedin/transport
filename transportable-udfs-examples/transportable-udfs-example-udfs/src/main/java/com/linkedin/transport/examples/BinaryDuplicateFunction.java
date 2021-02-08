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


public class BinaryDuplicateFunction extends StdUDF1<ByteBuffer, ByteBuffer> implements TopLevelStdUDF  {
  @Override
  public ByteBuffer eval(ByteBuffer byteBuffer) {
    ByteBuffer results = ByteBuffer.allocate(2 * byteBuffer.array().length);
    for (int i = 0; i < 2; i++) {
      for (byte b : byteBuffer.array()) {
        results.put(b);
      }
    }
    return results;
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of("varbinary");
  }

  @Override
  public String getOutputParameterSignature() {
    return "varbinary";
  }

  @Override
  public String getFunctionName() {
    return "binary_duplicate";
  }

  @Override
  public String getFunctionDescription() {
    return "Duplicate a binary object";
  }
}
