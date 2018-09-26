/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.examples.presto;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BooleanType.*;
import static com.facebook.presto.spi.type.IntegerType.*;
import static com.facebook.presto.spi.type.VarcharType.*;
import static com.facebook.presto.type.UnknownType.*;


public class TestArrayElementAtFunctionWrapper extends AbstractTestFunctions {

  @BeforeClass
  public void registerFunction() {
    registerScalarFunction(new ArrayElementAtFunctionWrapper());
  }

  @Test
  public void testArrayElementAt() {

    assertFunction("cast(array_element_at(array['1', '2'], 1) as varchar)", VARCHAR, "2");

    assertFunction("array_element_at(array[1, 2], 1)", INTEGER, 2);

    assertFunction("array_element_at(array[true, false], 1)", BOOLEAN, false);

    assertFunction("cast(array_element_at(array[array['1'], array['2']], 1) as array(varchar))", new ArrayType(VARCHAR),
        ImmutableList.of("2"));

    assertFunction("array_element_at(null, 1)", UNKNOWN, null);

    assertFunction("array_element_at(array[1], null)", INTEGER, null);
  }
}
