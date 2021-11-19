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


public class TestNumericAddFunction extends AbstractUDFTest {

  @Override
  protected Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> getTopLevelUDFClassesAndImplementations() {
    return ImmutableMap.of(NumericAddFunction.class,
        ImmutableList.of(
            NumericAddIntFunction.class,
            NumericAddLongFunction.class,
            NumericAddFloatFunction.class,
            NumericAddDoubleFunction.class));
  }

  @Test
  public void testNumericAdd() {
    Tester tester = getTester();
    tester.check(functionCall("numeric_add", 1, 2), 3, "integer");
    tester.check(functionCall("numeric_add", 1L, 2L), 3L, "bigint");
    tester.check(functionCall("numeric_add", 3.0, 4.0), 7.0, "double");

    Object expectedResult;
    if (tester.getClass().getCanonicalName().contains("HiveTester")) {
      // Note that org.apache.hive.service.cli.Column.addValue() converts any elements in RowSet from float to double
      expectedResult = 5.0;
    } else {
      expectedResult = 5.0f;
    }
    tester.check(functionCall("numeric_add", 2.0f, 3.0f), expectedResult, "real");
  }
}
