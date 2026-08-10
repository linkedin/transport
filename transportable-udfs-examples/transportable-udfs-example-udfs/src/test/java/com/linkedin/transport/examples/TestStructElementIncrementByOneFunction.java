/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
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


public class TestStructElementIncrementByOneFunction extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(
        StructElementIncrementByOneFunction.class, ImmutableList.of(StructElementIncrementByOneFunction.class));
  }

  @Test
  public void testStructElementIncrementByOneFunction() {
    StdTester tester = getTester();
    tester.check(functionCall("struct_element_increment_by_one", row(1, 3)), row(2, 3), "row(integer,integer)");
    tester.check(functionCall("struct_element_increment_by_one", row(-2, 3)), row(-1, 3), "row(integer,integer)");
  }
}