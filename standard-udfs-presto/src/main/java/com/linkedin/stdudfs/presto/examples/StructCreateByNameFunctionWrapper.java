package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.linkedin.stdudfs.api.udf.StdUDF;
import com.linkedin.stdudfs.examples.StructCreateByNameFunction;
import com.linkedin.stdudfs.presto.StdUdfWrapper;
import io.airlift.slice.Slice;


// Suppressing method and argument naming convention since this code is supposed to be auto-generated. By design,
// the generated method name is assigned from the TopLevelStdUDF's getFunctionName() method, which does not have
// to adhere to standard Java method naming conventions.
@SuppressWarnings({"checkstyle:methodname", "checkstyle:regexpsinglelinejava"})
public class StructCreateByNameFunctionWrapper extends StdUdfWrapper {
  public StructCreateByNameFunctionWrapper() {
    super(new StructCreateByNameFunction());
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Void field1Value, Slice field2Name, Void field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Void field1Value, Slice field2Name, long field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Void field1Value, Slice field2Name, boolean field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Void field1Value, Slice field2Name, Slice field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Void field1Value, Slice field2Name, Block field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, long field1Value, Slice field2Name, Void field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, long field1Value, Slice field2Name, long field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, long field1Value, Slice field2Name, boolean field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, long field1Value, Slice field2Name, Slice field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, long field1Value, Slice field2Name, Block field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, boolean field1Value, Slice field2Name, Void field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, boolean field1Value, Slice field2Name, long field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, boolean field1Value, Slice field2Name, boolean field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, boolean field1Value, Slice field2Name, Slice field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, boolean field1Value, Slice field2Name, Block field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Slice field1Value, Slice field2Name, Void field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Slice field1Value, Slice field2Name, long field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Slice field1Value, Slice field2Name, boolean field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Slice field1Value, Slice field2Name, Slice field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Slice field1Value, Slice field2Name, Block field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Block field1Value, Slice field2Name, Void field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Block field1Value, Slice field2Name, long field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Block field1Value, Slice field2Name, boolean field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Block field1Value, Slice field2Name, Slice field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @UsedByGeneratedCode
  public Block struct_create_by_name_Block(StdUDF stdUDF, Type arg1Type, Type arg2Type, Type arg3Type,
      Type arg4Type, Slice field1Name, Block field1Value, Slice field2Name, Block field2Value) {
    return (Block) eval(stdUDF, arg1Type, arg2Type, arg3Type, arg4Type, field1Name, field1Value, field2Name,
        field2Value);
  }

  @Override
  protected StdUDF getStdUDF() {
    return new StructCreateByNameFunction();
  }
}
