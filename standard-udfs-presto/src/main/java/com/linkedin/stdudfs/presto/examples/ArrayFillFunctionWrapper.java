package com.linkedin.stdudfs.presto.examples;

import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.ArrayFillFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


public class ArrayFillFunctionWrapper extends StdUdfWrapper {
  public ArrayFillFunctionWrapper() {
    super(new ArrayFillFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new ArrayFillFunction();
  }
}
