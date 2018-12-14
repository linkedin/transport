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


public class TestArrayElementAtFunction extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(ArrayElementAtFunction.class, ImmutableList.of(ArrayElementAtFunction.class));
  }

  @Test
  public void testArrayElementAt() {
    StdTester tester = getTester();
    tester.check(functionCall("array_element_at", array("1", "2"), 1), "2", "varchar");
    tester.check(functionCall("array_element_at", array(1, 2), 1), 2, "integer");
    tester.check(functionCall("array_element_at", array(true, false), 1), false, "boolean");
    tester.check(functionCall("array_element_at", array(array("1"), array("2")), 1), array("2"), "array(varchar)");
    tester.check(functionCall("array_element_at", null, 1), null, "unknown");
    tester.check(functionCall("array_element_at", array(1), null), null, "integer");
    tester.check(functionCall("array_element_at", array("1", null, "2"), 1), null, "varchar");
    tester.check(functionCall("array_element_at", array(array("1"), array("2", null)), 1), array("2", null),
        "array(varchar)");
  }
}
