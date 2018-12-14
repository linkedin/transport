/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.udf.StdUDF;
import com.linkedin.transport.api.udf.TopLevelStdUDF;
import com.linkedin.transport.test.AbstractStdUDFTest;
import com.linkedin.transport.test.spi.StdTester;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TestMapFromTwoArraysFunctionNested extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(MapFromTwoArraysFunction.class, ImmutableList.of(MapFromTwoArraysFunction.class),
        MapKeySetFunction.class, ImmutableList.of(MapKeySetFunction.class), MapValuesFunction.class,
        ImmutableList.of(MapValuesFunction.class));
  }

  @Test
  public void testMapFromTwoArraysFunctionNested() {
    StdTester tester = getTester();
    tester.check(functionCall("map_from_two_arrays", functionCall("std_map_values", map(1, "a", 2, "b")),
        functionCall("map_key_set", map(1, "a", 2, "b"))), map("a", 1, "b", 2), "map(varchar, integer)");

    tester.check(functionCall("map_from_two_arrays", functionCall("std_map_values", map(1, "a", 2, "b")),
        functionCall("map_key_set", map(1, "a", 2, "b"))), map("a", 1, "b", 2), "map(varchar, integer)");
  }
}
