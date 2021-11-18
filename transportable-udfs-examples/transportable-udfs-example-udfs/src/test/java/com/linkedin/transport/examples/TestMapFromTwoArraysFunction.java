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
import com.linkedin.transport.test.AbstractUDFTest;
import com.linkedin.transport.test.spi.Tester;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TestMapFromTwoArraysFunction extends AbstractUDFTest {

  @Override
  protected Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> getTopLevelUDFClassesAndImplementations() {
    return ImmutableMap.of(MapFromTwoArraysFunction.class, ImmutableList.of(MapFromTwoArraysFunction.class));
  }

  @Test
  public void testMapFromTwoArraysFunction() {
    Tester tester = getTester();
    tester.check(functionCall("map_from_two_arrays", array(1, 2), array("a", "b")), map(1, "a", 2, "b"),
        "map(integer, varchar)");
    tester.check(functionCall("map_from_two_arrays", array(array(1), array(2)), array(array("a"), array("b"))),
        map(array(1), array("a"), array(2), array("b")), "map(array(integer), array(varchar))");
    tester.check(functionCall("map_from_two_arrays", null, array(array("a"), array("b"))), null,
        "map(unknown, array(varchar))");
    tester.check(functionCall("map_from_two_arrays", array(array(1), array(2)), null), null,
        "map(array(integer), unknown)");
  }
}
