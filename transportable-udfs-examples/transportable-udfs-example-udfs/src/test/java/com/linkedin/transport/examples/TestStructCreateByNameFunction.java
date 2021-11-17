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


public class TestStructCreateByNameFunction extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> getTopLevelUDFClassesAndImplementations() {
    return ImmutableMap.of(StructCreateByNameFunction.class, ImmutableList.of(StructCreateByNameFunction.class));
  }

  @Test
  public void testStructCreateByNameFunction() {
    StdTester tester = getTester();
    tester.check(functionCall("struct_create_by_name", "a", "x", "b", "y"), row("x", "y"), "row(varchar,varchar)");
    tester.check(functionCall("struct_create_by_name", null, "x", "b", "y"), null, "row(varchar,varchar)");
    tester.check(functionCall("struct_create_by_name", "a", "x", null, "y"), null, "row(varchar,varchar)");
    tester.check(functionCall("struct_create_by_name", "a", null, "b", "y"), null, "row(unknown,varchar)");
  }
}
