/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.presto;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.RowType;
import com.google.common.collect.ImmutableList;
import java.util.Optional;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.*;
import static com.facebook.presto.type.UnknownType.*;


public class TestStructCreateByNameFunctionWrapper extends AbstractTestFunctions {

  public static final RowType ROW_TYPE =
      RowType.from(ImmutableList.of(new RowType.Field(Optional.of("a"), VARCHAR),
          new RowType.Field(Optional.of("b"), VARCHAR)));
  public static final RowType UNKNOWN_TYPE =
      RowType.from(ImmutableList.of(new RowType.Field(Optional.of("a"), UNKNOWN),
          new RowType.Field(Optional.of("b"), UNKNOWN)));

  @BeforeClass
  public void registerFunction() {
    registerScalarFunction(new StructCreateByNameFunctionWrapper());
  }

  @Test
  public void testStructCreateByNameFunction() {

    assertFunction("cast(struct_create_by_name('a', 'x', 'b', 'y') as row(a varchar, b varchar)).a", VARCHAR, "x");

    assertFunction("cast(struct_create_by_name('a', 'x', 'b', 'y') as row(a varchar, b varchar)).b", VARCHAR, "y");

    assertFunction("cast(struct_create_by_name(null, 'x', 'b', 'y') as row(a varchar, b varchar))", ROW_TYPE, null);

    assertFunction("cast(struct_create_by_name('a', 'x', null, 'y') as row(a varchar, b varchar))", ROW_TYPE, null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testStructCreateByNameFunctionError1() {
    assertFunction("cast(struct_create_by_name('a', null, 'b', 'y') as row(a varchar, b varchar))", UNKNOWN_TYPE, null);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testStructCreateByNameFunctionError2() {
    assertFunction("cast(struct_create_by_name('a', 'x', 'b', null) as row(a varchar, b varchar))", UNKNOWN_TYPE, null);
  }
}
