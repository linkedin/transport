/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.api.udf.UDF2;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.util.List;


public class ArrayFillFunction<K> extends UDF2<K, Long, ArrayData<K>> implements TopLevelUDF {

  private DataType _arrayType;

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
  public void init(TypeFactory typeFactory) {
    super.init(typeFactory);
    _arrayType = getTypeFactory().createDataType(getOutputParameterSignature());
  }

  @Override
  public ArrayData<K> eval(K a, Long length) {
    ArrayData<K> array = getTypeFactory().createArray(_arrayType);
    for (int i = 0; i < length; i++) {
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
