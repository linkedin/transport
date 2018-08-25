package com.linkedin.stdudfs.presto.examples;

import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.StructCreateByIndexFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


public class StructCreateByIndexFunctionWrapper extends StdUdfWrapper {
  public StructCreateByIndexFunctionWrapper() {
    super(new StructCreateByIndexFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new StructCreateByIndexFunction();
  }
}
