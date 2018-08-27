package com.linkedin.stdudfs.presto.examples;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.IntegerType.*;
import static com.facebook.presto.spi.type.VarcharType.*;
import static com.facebook.presto.type.UnknownType.*;


public class TestMapValuesFunctionWrapper extends AbstractTestFunctions {

  @BeforeClass
  public void registerFunction() {
    registerScalarFunction(new MapValuesFunctionWrapper());
  }

  @Test
  public void testMapValues() {

    assertFunction("std_map_values(map(array[1, 2, 3], array[4, 5, 6]))", new ArrayType(INTEGER),
        ImmutableList.of(4, 5, 6));

    assertFunction("cast (std_map_values(map(array['1', '2', '3'], array['4', '5', '6'])) as array(varchar))",
        new ArrayType(VARCHAR), ImmutableList.of("4", "5", "6"));

    assertFunction("std_map_values(null)", new ArrayType(UNKNOWN), null);
  }
}
