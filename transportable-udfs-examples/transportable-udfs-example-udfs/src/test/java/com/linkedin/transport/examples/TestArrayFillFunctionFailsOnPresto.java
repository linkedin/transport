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
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TestArrayFillFunctionFailsOnPresto extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(ArrayFillFunction.class, ImmutableList.of(ArrayFillFunction.class));
  }

  @Test
  public void testArrayFillFailsOnPresto() {
    // TODO: We don't have good support in Presto for null input value with unkown non-nullable type
    getTester().check(functionCall("array_fill", null, 2L), null, "array(unknown)");
  }
}
