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


public class TestMapValuesFunction extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(MapValuesFunction.class, ImmutableList.of(MapValuesFunction.class));
  }

  @Test
  public void testMapValues() {
    StdTester tester = getTester();
    tester.check(functionCall("std_map_values", map(1, 4, 2, 5, 3, 6)), array(4, 5, 6), "array(integer)");
    tester.check(functionCall("std_map_values", map("1", "4", "2", "5", "3", "6")), array("4", "5", "6"),
        "array(varchar)");
    tester.check(functionCall("std_map_values", (Object) null), null, "array(unknown)");
  }
}
