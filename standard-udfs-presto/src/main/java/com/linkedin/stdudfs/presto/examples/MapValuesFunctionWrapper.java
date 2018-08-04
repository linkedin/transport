package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.MapValuesFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


// Suppressing method and argument naming convention since this code is supposed to be auto-generated. By design,
// the generated method name is assigned from the TopLevelStdUDF's getFunctionName() method, which does not have
// to adhere to standard Java method naming conventions.
@SuppressWarnings({"checkstyle:methodname", "checkstyle:regexpsinglelinejava"})
public class MapValuesFunctionWrapper extends StdUdfWrapper {
  public MapValuesFunctionWrapper() {
    super(new MapValuesFunction());
  }

  @UsedByGeneratedCode
  public Block std_map_values_Block(StdUDF stdUDF, Type arg1Type, Block arg1) {
    return (Block) eval(stdUDF, arg1Type, arg1);
  }

  @Override
  protected StdUDF getStdUDF() {
    return new MapValuesFunction();
  }
}
