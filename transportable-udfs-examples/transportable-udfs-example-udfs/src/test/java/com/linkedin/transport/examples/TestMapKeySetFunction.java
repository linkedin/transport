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


public class TestMapKeySetFunction extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(MapKeySetFunction.class, ImmutableList.of(MapKeySetFunction.class));
  }

  @Test
  public void testMapKeySet() {
    StdTester tester = getTester();
    tester.check(functionCall("map_key_set", map(1, 4, 2, 5, 3, 6)), array(1, 2, 3), "array(integer)");
    tester.check(functionCall("map_key_set", map("1", "4", "2", "5", "3", "6")), array("1", "2", "3"),
        "array(varchar)");
    tester.check(functionCall("map_key_set", (Object) null), null, "array(unknown)");
  }
}
