package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.MapKeySetFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;


// Suppressing method and argument naming convention since this code is supposed to be auto-generated. By design,
// the generated method name is assigned from the TopLevelStdUDF's getFunctionName() method, which does not have
// to adhere to standard Java method naming conventions.
@SuppressWarnings({"checkstyle:methodname", "checkstyle:regexpsinglelinejava"})
public class MapKeySetFunctionWrapper extends StdUdfWrapper {
  public MapKeySetFunctionWrapper() {
    super(new MapKeySetFunction());
  }

  @UsedByGeneratedCode
  public Block map_key_set_Block(StdUDF stdUDF, Type arg1Type, Block arg1) {
    return (Block) eval(stdUDF, arg1Type, arg1);
  }

  @Override
  protected StdUDF getStdUDF() {
    return new MapKeySetFunction();
  }
}
