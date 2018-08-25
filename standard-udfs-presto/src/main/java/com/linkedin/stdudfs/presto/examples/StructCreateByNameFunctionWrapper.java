package com.linkedin.stdudfs.presto.examples;

import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.StructCreateByNameFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


public class StructCreateByNameFunctionWrapper extends StdUdfWrapper {
  public StructCreateByNameFunctionWrapper() {
    super(new StructCreateByNameFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new StructCreateByNameFunction();
  }
}
