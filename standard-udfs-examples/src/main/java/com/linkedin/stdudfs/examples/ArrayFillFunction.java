/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.linkedin.stdudfs.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.data.StdLong;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.api.udf.StdUDF2;
import com.linkedin.stdudfs.api.udf.TopLevelStdUDF;
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
