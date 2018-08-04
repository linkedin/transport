package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.*;
import static com.facebook.presto.type.UnknownType.*;


public class TestStructCreateByNameFunctionWrapper extends AbstractTestFunctions {

  @BeforeClass
  public void registerFunction() {
    registerScalarFunction(new StructCreateByNameFunctionWrapper());
  }

  @Test
  public void testStructCreateByNameFunction() {

    assertFunction("cast(struct_create_by_name('a', 'x', 'b', 'y') as row(a varchar, b varchar)).a", VARCHAR, "x");

    assertFunction("cast(struct_create_by_name('a', 'x', 'b', 'y') as row(a varchar, b varchar)).b", VARCHAR, "y");

    assertFunction("cast(struct_create_by_name(null, 'x', 'b', 'y') as row(A varchar, B varchar))",
        new RowType(ImmutableList.of(VARCHAR, VARCHAR), Optional.of(ImmutableList.of("a", "b"))), null);

    assertFunction("cast(struct_create_by_name('a', 'x', null, 'y') as row(A varchar, B varchar))",
        new RowType(ImmutableList.of(VARCHAR, VARCHAR), Optional.of(ImmutableList.of("a", "b"))), null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testStructCreateByNameFunctionError1() {
    assertFunction("cast(struct_create_by_name('a', null, 'b', 'y') as row(A varchar, B varchar))",
        new RowType(ImmutableList.of(UNKNOWN, UNKNOWN), Optional.of(ImmutableList.of("a", "b"))), null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testStructCreateByNameFunctionError2() {
    assertFunction("cast(struct_create_by_name('a', 'x', 'b', null) as row(A varchar, B varchar))",
        new RowType(ImmutableList.of(UNKNOWN, UNKNOWN), Optional.of(ImmutableList.of("a", "b"))), null);
  }
}
