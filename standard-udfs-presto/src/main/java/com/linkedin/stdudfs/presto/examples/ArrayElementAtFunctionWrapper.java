package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.ArrayElementAtFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;
import io.airlift.slice.Slice;


// Suppressing method and argument naming convention since this code is supposed to be auto-generated. By design,
// the generated method name is assigned from the TopLevelStdUDF's getFunctionName() method, which does not have
// to adhere to standard Java method naming conventions.
@SuppressWarnings({"checkstyle:methodname", "checkstyle:regexpsinglelinejava"})
public class ArrayElementAtFunctionWrapper extends StdUdfWrapper {
  public ArrayElementAtFunctionWrapper() {
    super(new ArrayElementAtFunction());
  }

  @UsedByGeneratedCode
  public Void array_element_at_Void(StdUDF stdUDF, Type arg1Type, Type arg2Type, Block array, long idx) {
    return null;
  }

  @UsedByGeneratedCode
  public Long array_element_at_Long(StdUDF stdUDF, Type arg1Type, Type arg2Type, Block array, long idx) {
    // The case below is interesting and should be taken care in the wrapper auto-gen code.
    // Basically, the generic function (i.e., array_element_at_Generic()) can return Integer or Long
    // depending on the data type of the array element (i.e., Integer or BigInt respectively).
    // However, we always need to cast to Long. Therefore, we cast to Number then call longValue().
    return ((Number) eval(stdUDF, arg1Type, arg2Type, array, idx)).longValue();
  }

  @UsedByGeneratedCode
  public Boolean array_element_at_Boolean(StdUDF stdUDF, Type arg1Type, Type arg2Type, Block array, long idx) {
    return (Boolean) eval(stdUDF, arg1Type, arg2Type, array, idx);
  }

  @UsedByGeneratedCode
  public Block array_element_at_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Block array, long idx) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, array, idx);
  }

  @UsedByGeneratedCode
  public Slice array_element_at_Slice(StdUDF stdUDF, Type arg1Type, Type arg2Type, Block array, long idx) {
    return (Slice) eval(stdUDF, arg1Type, arg2Type, array, idx);
  }

  @Override
  protected StdUDF getStdUDF() {
    return new ArrayElementAtFunction();
  }
}
