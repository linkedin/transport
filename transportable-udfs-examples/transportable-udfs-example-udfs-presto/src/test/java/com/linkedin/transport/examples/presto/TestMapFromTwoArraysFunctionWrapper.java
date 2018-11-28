/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.presto;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeParameter;
import com.facebook.presto.type.MapParametricType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.IntegerType.*;
import static com.facebook.presto.spi.type.VarcharType.*;
import static com.facebook.presto.type.UnknownType.*;


public class TestMapFromTwoArraysFunctionWrapper extends AbstractTestFunctions {

  @BeforeClass
  public void registerFunction() {
    registerScalarFunction(new MapFromTwoArraysFunctionWrapper());
  }

  @Test
  public void testMapFromTwoArraysFunction() {

    assertFunction("cast(map_from_two_arrays(array[1, 2], array['a', 'b']) as map(integer, varchar))",
        createMapType(INTEGER, VARCHAR), ImmutableMap.of(1, "a", 2, "b"));

    assertFunction("map_from_two_arrays(array[array[1], array[2]], array[array['a'], array['b']])",
        createMapType(new ArrayType(INTEGER), new ArrayType(createVarcharType(1))),
        ImmutableMap.of(ImmutableList.of(1), ImmutableList.of("a"), ImmutableList.of(2), ImmutableList.of("b")));

    assertFunction("map_from_two_arrays(null, array[array['a'], array['b']])",
        createMapType(UNKNOWN, new ArrayType(createVarcharType(1))), null);

    assertFunction("map_from_two_arrays(array[array[1], array[2]], null)", createMapType(new ArrayType(INTEGER), UNKNOWN),
        null);
  }

  private MapType createMapType(Type keyType, Type valueType) {
    TypeManager typeManager = this.functionAssertions.getMetadata().getTypeManager();
    return (MapType) new MapParametricType().createType(typeManager,
        ImmutableList.of(TypeParameter.of(keyType), TypeParameter.of(valueType)));
  }
}
