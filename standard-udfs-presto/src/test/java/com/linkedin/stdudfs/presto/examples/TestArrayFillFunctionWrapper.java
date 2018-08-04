package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.*;
import static com.facebook.presto.spi.type.IntegerType.*;
import static com.facebook.presto.spi.type.VarcharType.*;
import static com.facebook.presto.type.UnknownType.*;


public class TestArrayFillFunctionWrapper extends AbstractTestFunctions {

  @BeforeClass
  public void registerFunction() {
    registerScalarFunction(new ArrayFillFunctionWrapper());
  }

  @Test
  public void testArrayFill() {

    assertFunction("array_fill(1, 5)", new ArrayType(INTEGER), ImmutableList.of(1, 1, 1, 1, 1));

    assertFunction("array_fill('1', 5)", new ArrayType(createVarcharType(1)),
        ImmutableList.of("1", "1", "1", "1", "1"));
    assertFunction("array_fill(true, 5)", new ArrayType(BOOLEAN), ImmutableList.of(true, true, true, true, true));

    assertFunction("array_fill(array[1], 5)", new ArrayType(new ArrayType(INTEGER)),
        ImmutableList.of(ImmutableList.of(1), ImmutableList.of(1), ImmutableList.of(1), ImmutableList.of(1),
            ImmutableList.of(1)));

    assertFunction("array_fill(1, 0)", new ArrayType(INTEGER), ImmutableList.of());

    assertFunction("array_fill(1, null)", new ArrayType(INTEGER), null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testArrayFillUnkownNonNullable() {
    assertFunction("array_fill(null, 1)", new ArrayType(UNKNOWN), null);
  }
}
