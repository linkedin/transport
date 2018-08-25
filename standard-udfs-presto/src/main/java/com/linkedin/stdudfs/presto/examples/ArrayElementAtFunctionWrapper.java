package com.linkedin.stdudfs.presto.examples;

import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.ArrayElementAtFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


public class ArrayElementAtFunctionWrapper extends StdUdfWrapper {
  public ArrayElementAtFunctionWrapper() {
    super(new ArrayElementAtFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new ArrayElementAtFunction();
  }
}
