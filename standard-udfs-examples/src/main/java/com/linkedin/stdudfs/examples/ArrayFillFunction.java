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
