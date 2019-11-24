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


public class TestArrayFillFunction extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(ArrayFillFunction.class, ImmutableList.of(ArrayFillFunction.class));
  }

  @Test
  public void testArrayFill() {
    StdTester tester = getTester();
    tester.check(functionCall("array_fill", 1, 5L), array(1, 1, 1, 1, 1), "array(integer)");
    tester.check(functionCall("array_fill", "1", 5L), array("1", "1", "1", "1", "1"), "array(varchar)");
    tester.check(functionCall("array_fill", true, 5L), array(true, true, true, true, true), "array(boolean)");
    tester.check(functionCall("array_fill", array(1), 5L), array(array(1), array(1), array(1), array(1), array(1)),
        "array(array(integer))");
    tester.check(functionCall("array_fill", 1, 0L), array(), "array(integer)");
    tester.check(functionCall("array_fill", 1, null), null, "array(integer)");
    tester.check(functionCall("array_fill", null, 2L), null, "array(unknown)");
  }
}
