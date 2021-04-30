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


public class TestNestedMapFromTwoArraysFunction extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(NestedMapFromTwoArraysFunction.class, ImmutableList.of(NestedMapFromTwoArraysFunction.class));
  }

  @Test
  public void testNestedMapUnionFunction() {
    StdTester tester = getTester();
    tester.check(
        functionCall("nested_map_from_two_arrays", array(row(array(1, 2, 3), array("a", "b", "c")))),
        array(row(map(1, "a", 2, "b", 3, "c"))),
        "array(row(map(integer,varchar)))");
  }
}
