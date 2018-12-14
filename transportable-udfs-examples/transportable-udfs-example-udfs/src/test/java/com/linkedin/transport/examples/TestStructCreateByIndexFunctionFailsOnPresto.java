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


public class TestStructCreateByIndexFunctionFailsOnPresto extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(StructCreateByIndexFunction.class, ImmutableList.of(StructCreateByIndexFunction.class));
  }

  @Test
  public void testStructCreateByIndexFunctionFailsOnPresto() {
    StdTester tester = getTester();
    // TODO: We don't have good support in Presto for null input value with unkown non-nullable type
    tester.check(functionCall("struct_create_by_index", null, null), null, "row(unknown,unknown)");
  }
}
