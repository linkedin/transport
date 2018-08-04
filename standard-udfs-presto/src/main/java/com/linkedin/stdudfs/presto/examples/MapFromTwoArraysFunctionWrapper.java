package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.MapFromTwoArraysFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


// Suppressing method and argument naming convention since this code is supposed to be auto-generated. By design,
// the generated method name is assigned from the TopLevelStdUDF's getFunctionName() method, which does not have
// to adhere to standard Java method naming conventions.
@SuppressWarnings({"checkstyle:methodname", "checkstyle:regexpsinglelinejava"})
public class MapFromTwoArraysFunctionWrapper extends StdUdfWrapper {
  public MapFromTwoArraysFunctionWrapper() {
    super(new MapFromTwoArraysFunction());
  }

  @UsedByGeneratedCode
  public Block map_from_two_arrays_Block(StdUDF stdUDF, Type array1Type, Type array2Type, Block array1,
      Block array2) {
    return (Block) eval(stdUDF, array1Type, array2Type, array1, array2);
  }

  @Override
  protected StdUDF getStdUDF() {
    return new MapFromTwoArraysFunction();
  }
}
