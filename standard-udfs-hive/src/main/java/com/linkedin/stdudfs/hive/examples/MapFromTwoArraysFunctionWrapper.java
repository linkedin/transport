package com.linkedin.stdudfs.hive.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.api.udf.TopLevelStdUDF;
import com.linkedin.stdudfs.examples.MapFromTwoArraysFunction;
import com.linkedin.stdudfs.hive.StdUdfWrapper;
import java.util.List;


public class MapFromTwoArraysFunctionWrapper extends StdUdfWrapper {
  @Override
  protected List<? extends StdUDF> getStdUdfImplementations() {
    return ImmutableList.of(new MapFromTwoArraysFunction());
  }

  @Override
  protected Class<? extends TopLevelStdUDF> getTopLevelUdfClass() {
    return MapFromTwoArraysFunction.class;
  }
}
