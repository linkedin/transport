package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.ArrayFillFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;
import io.airlift.slice.Slice;


// Suppressing method and argument naming convention since this code is supposed to be auto-generated. By design,
// the generated method name is assigned from the TopLevelStdUDF's getFunctionName() method, which does not have
// to adhere to standard Java method naming conventions.
@SuppressWarnings({"checkstyle:methodname", "checkstyle:regexpsinglelinejava"})
public class ArrayFillFunctionWrapper extends StdUdfWrapper {
  public ArrayFillFunctionWrapper() {
    super(new ArrayFillFunction());
  }

  @UsedByGeneratedCode
  public Block array_fill_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Void a, long length) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, a, length);
  }

  @UsedByGeneratedCode
  public Block array_fill_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Long a, long length) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, a, length);
  }

  @UsedByGeneratedCode
  public Block array_fill_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, long a, long length) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, a, length);
  }

  @UsedByGeneratedCode
  public Block array_fill_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Boolean a, long length) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, a, length);
  }

  @UsedByGeneratedCode
  public Block array_fill_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, boolean a, long length) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, a, length);
  }

  @UsedByGeneratedCode
  public Block array_fill_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Slice a, long length) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, a, length);
  }

  @UsedByGeneratedCode
  public Block array_fill_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Block a, long length) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, a, length);
  }

  @Override
  protected StdUDF getStdUDF() {
    return new ArrayFillFunction();
  }
}
