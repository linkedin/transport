package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.StructCreateByIndexFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;
import io.airlift.slice.Slice;


// Suppressing method and argument naming convention since this code is supposed to be auto-generated. By design,
// the generated method name is assigned from the TopLevelStdUDF's getFunctionName() method, which does not have
// to adhere to standard Java method naming conventions.
@SuppressWarnings({"checkstyle:methodname", "checkstyle:regexpsinglelinejava"})
public class StructCreateByIndexFunctionWrapper extends StdUdfWrapper {
  public StructCreateByIndexFunctionWrapper() {
    super(new StructCreateByIndexFunction());
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Void field1Value,
      Void field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Void field1Value,
      long field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Void field1Value,
      boolean field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Void field1Value,
      Slice field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Void field1Value,
      Block field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, long field1Value,
      Void field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, long field1Value,
      long field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, long field1Value,
      boolean field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, long field1Value,
      Slice field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, long field1Value,
      Block field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type,
      boolean field1Value, Void field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type,
      boolean field1Value, long field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type,
      boolean field1Value, boolean field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type,
      boolean field1Value, Slice field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type,
      boolean field1Value, Block field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Slice field1Value,
      Void field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Slice field1Value,
      long field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Slice field1Value,
      boolean field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Slice field1Value,
      Slice field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Slice field1Value,
      Block field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Block field1Value,
      Void field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Block field1Value,
      long field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Block field1Value,
      boolean field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Block field1Value,
      Slice field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_index_Block(StdUDF stdUDF, Type field1Type, Type field2Type, Block field1Value,
      Block field2Value) {
    return (Block) eval(stdUDF, field1Type, field2Type, field1Value, field2Value);
  }

  @Override
  protected StdUDF getStdUDF() {
    return new StructCreateByIndexFunction();
  }
}
