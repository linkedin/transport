package com.linkedin.stdudfs.presto.examples;

import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.MapValuesFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


public class MapValuesFunctionWrapper extends StdUdfWrapper {
  public MapValuesFunctionWrapper() {
    super(new MapValuesFunction());
  }

  @Override
  protected StdUDF getStdUDF() {
    return new MapValuesFunction();
  }
}
