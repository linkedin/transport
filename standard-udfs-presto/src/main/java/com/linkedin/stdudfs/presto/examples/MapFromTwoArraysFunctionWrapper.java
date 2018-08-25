package com.linkedin.stdudfs.presto.examples;

import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.MapFromTwoArraysFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


public class MapFromTwoArraysFunctionWrapper extends StdUdfWrapper {
  public MapFromTwoArraysFunctionWrapper() {
    super(new MapFromTwoArraysFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new MapFromTwoArraysFunction();
  }
}
