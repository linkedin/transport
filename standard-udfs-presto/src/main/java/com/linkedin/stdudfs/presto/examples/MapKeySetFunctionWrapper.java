package com.linkedin.stdudfs.presto.examples;

import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.MapKeySetFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


public class MapKeySetFunctionWrapper extends StdUdfWrapper {
  public MapKeySetFunctionWrapper() {
    super(new MapKeySetFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new MapKeySetFunction();
  }
}
