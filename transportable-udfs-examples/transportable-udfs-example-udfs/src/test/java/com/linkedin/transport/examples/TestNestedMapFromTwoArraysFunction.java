/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.api.udf.TopLevelUDF;
import com.linkedin.transport.test.AbstractStdUDFTest;
import com.linkedin.transport.test.spi.StdTester;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TestNestedMapFromTwoArraysFunction extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> getTopLevelUDFClassesAndImplementations() {
    return ImmutableMap.of(NestedMapFromTwoArraysFunction.class, ImmutableList.of(NestedMapFromTwoArraysFunction.class));
  }

  @Test
  public void testNestedMapUnionFunction() {
    StdTester tester = getTester();
    tester.check(
        functionCall("nested_map_from_two_arrays", array(row(array(1, 2), array("a", "b")))),
        array(row(map(1, "a", 2, "b"))),
        "array(row(map(integer,varchar)))");
    tester.check(
        functionCall("nested_map_from_two_arrays", array(row(array(1, 2), array("a", "b")), row(array(11, 12), array("aa", "bb")))),
        array(row(map(1, "a", 2, "b")), row(map(11, "aa", 12, "bb"))),
        "array(row(map(integer,varchar)))");
    tester.check(
        functionCall("nested_map_from_two_arrays",
            array(row(array(array(1), array(2)), array(array("a"), array("b"))))),
        array(row(map(array(1), array("a"), array(2), array("b")))),
        "array(row(map(array(integer),array(varchar))))");
    tester.check(
        functionCall("nested_map_from_two_arrays",  array(row(array(1), array("a", "b")))),
        null, "array(row(map(integer,varchar)))");
    tester.check(
        functionCall("nested_map_from_two_arrays",  array(row(null, array("a", "b")))),
        null, "array(row(map(unknown,varchar)))");
  }
}
