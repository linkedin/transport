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


public class TestFileLookupFunction extends AbstractUDFTest {

  @Override
  protected Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> getTopLevelUDFClassesAndImplementations() {
    return ImmutableMap.of(FileLookupFunction.class, ImmutableList.of(FileLookupFunction.class));
  }

  @Test
  public void testFileLookup() {
    Tester tester = getTester();
    tester.check(functionCall("file_lookup", resource("file_lookup_function/sample"), 1), true, "boolean");
    tester.check(functionCall("file_lookup", resource("file_lookup_function/sample"), 6), false, "boolean");
    tester.check(functionCall("file_lookup", null, 1), null, "boolean");
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testFileLookupFailNull() {
    Tester tester = getTester();
    tester.check(functionCall("file_lookup", resource("file_lookup_function/sample"), null), null, "boolean");
  }
}
