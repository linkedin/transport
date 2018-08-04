package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
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
        new MapType(INTEGER, VARCHAR), ImmutableMap.of(1, "a", 2, "b"));

    assertFunction("map_from_two_arrays(array[array[1], array[2]], array[array['a'], array['b']])",
        new MapType(new ArrayType(INTEGER), new ArrayType(createVarcharType(1))),
        ImmutableMap.of(ImmutableList.of(1), ImmutableList.of("a"), ImmutableList.of(2), ImmutableList.of("b")));

    assertFunction("map_from_two_arrays(null, array[array['a'], array['b']])",
        new MapType(UNKNOWN, new ArrayType(createVarcharType(1))), null);

    assertFunction("map_from_two_arrays(array[array[1], array[2]], null)", new MapType(new ArrayType(INTEGER), UNKNOWN),
        null);
  }
}
