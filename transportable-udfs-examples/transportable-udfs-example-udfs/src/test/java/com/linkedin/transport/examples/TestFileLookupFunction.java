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
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestFileLookupFunction extends AbstractStdUDFTest {

  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(FileLookupFunction.class, ImmutableList.of(FileLookupFunction.class));
  }

  @Test
  public void testFileLookup() {
    StdTester tester = getTester();
    tester.check(functionCall("file_lookup", resource("file_lookup_function/sample"), 1), true, "boolean");
    tester.check(functionCall("file_lookup", resource("file_lookup_function/sample"), 6), false, "boolean");
    tester.check(functionCall("file_lookup", null, 1), null, "boolean");
  }

  @Test
  public void testFileLookupFailNull() {
    try {
      StdTester tester = getTester();
      // in case of Trino, the execution of a query with UDF to check a null value in a file
      // does not result in a NullPointerException, but returns a null value
      tester.check(functionCall("file_lookup", resource("file_lookup_function/sample"), null), null, "boolean");
    } catch (NullPointerException ex) {
      // in case of Hive and Spark, the execution of a query with UDF to check a null value in a file results in a NullPointerException
      Assert.assertFalse(isTrinoTest());
    }
  }
}
