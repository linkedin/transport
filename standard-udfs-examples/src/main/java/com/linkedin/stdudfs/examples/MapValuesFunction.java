package com.linkedin.stdudfs.examples;

import com.google.common.collect.ImmutableList;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.data.StdMap;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.api.udf.StdUDF1;
import com.linkedin.stdudfs.api.udf.TopLevelStdUDF;
import java.util.List;


public class MapValuesFunction extends StdUDF1<StdMap, StdArray> implements TopLevelStdUDF {

  private StdType _mapType;

  @Override
  public List<String> getInputParameterSignatures() {
    return ImmutableList.of(
        "map(K,V)"
    );
  }

  @Override
  public String getOutputParameterSignature() {
    return "array(V)";
  }

  @Override
  public void init(StdFactory stdFactory) {
    super.init(stdFactory);
    _mapType = getStdFactory().createStdType(getOutputParameterSignature());
  }

  @Override
  public StdArray eval(StdMap map) {
    StdArray result = getStdFactory().createArray(_mapType);
    for (StdData value : map.values()) {
      result.add(value);
    }
    return result;
  }

  @Override
  public String getFunctionName() {
    return "std_map_values";
  }

  @Override
  public String getFunctionDescription() {
    return "Returns the set of values from a map";
  }
}
