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


public class TestStructCreateByIndexFunction extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> getTopLevelUDFClassesAndImplementations() {
    return ImmutableMap.of(StructCreateByIndexFunction.class, ImmutableList.of(StructCreateByIndexFunction.class));
  }

  @Test
  public void testStructCreateByIndexFunction() {
    StdTester tester = getTester();
    tester.check(functionCall("struct_create_by_index", "x", "y"), row("x", "y"), "row(varchar,varchar)");
    tester.check(functionCall("struct_create_by_index", 1, array(1)), row(1, array(1)), "row(integer,array(integer))");
    tester.check(functionCall("struct_create_by_index", null, null), null, "row(unknown,unknown)");
  }
}
