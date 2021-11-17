/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.udf.UDF2;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.util.List;


/**
 * Another way to define this class using generics can look like this
 *
 * public class ArrayElementAtFunction<K> extends UDF2<ArrayData<K>, Integer, K> implements TopLevelUDF {
 *
 *   @Override
 *   public K eval(ArrayData<K> a1, Integer idx) {
 *     return a1.get(idx);
 *   }
 *
 * }
 *
 */
public class ArrayElementAtFunction extends UDF2<ArrayData, Integer, Object> implements TopLevelUDF {

  @Override
  public String getFunctionName() {
    return "array_element_at";
  }

  @Override
  public String getFunctionDescription() {
    return "Create a map out of two arrays.";
  }

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "array(K)",
        "integer"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "K";
  }

  @Override
  public Object eval(ArrayData a1, Integer idx) {
    return a1.get(idx);
  }
}
