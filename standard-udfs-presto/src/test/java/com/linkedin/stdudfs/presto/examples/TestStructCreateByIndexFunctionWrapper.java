package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.IntegerType.*;
import static com.facebook.presto.spi.type.VarcharType.*;
import static com.facebook.presto.type.UnknownType.*;


public class TestStructCreateByIndexFunctionWrapper extends AbstractTestFunctions {

  @BeforeClass
  public void registerFunction() {
    registerScalarFunction(new StructCreateByIndexFunctionWrapper());
  }

  @Test
  public void testStructCreateByIndexFunction() {

    assertFunction("cast(struct_create_by_index('x', 'y') as row(a varchar, b varchar)).a", VARCHAR, "x");

    assertFunction("cast(struct_create_by_index('x', 'y') as row(a varchar, b varchar)).b", VARCHAR, "y");

    assertFunction("cast(struct_create_by_index(1, array[1]) as row(a integer, b array(integer))).a", INTEGER, 1);

    assertFunction("cast(struct_create_by_index(1, array[1]) as row(a integer, b array(integer))).b",
        new ArrayType(INTEGER), ImmutableList.of(1));

    assertFunction(
        "cast(struct_create_by_index(cast(null as integer), cast(null as integer)) as row(A integer, B integer))",
        new RowType(ImmutableList.of(INTEGER, INTEGER), Optional.of(ImmutableList.of("a", "b"))), null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testStructCreateByIndexFunctionError1() {
    assertFunction("struct_create_by_index(null, null)",
        new RowType(ImmutableList.of(UNKNOWN, UNKNOWN), Optional.of(ImmutableList.of("field0", "field1"))), null);
  }
}
