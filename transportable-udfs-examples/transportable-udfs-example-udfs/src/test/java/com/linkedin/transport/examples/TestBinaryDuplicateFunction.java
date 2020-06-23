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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;


public class TestBinaryDuplicateFunction extends AbstractStdUDFTest {
  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(BinaryDuplicateFunction.class, ImmutableList.of(BinaryDuplicateFunction.class));
  }

  @Test
  public void tesBinaryDuplicate() {
    StdTester tester = getTester();
    tesBinaryDuplicateHelper(tester, "bar", "barbar");
    tesBinaryDuplicateHelper(tester, "", "");
    tesBinaryDuplicateHelper(tester, "foobar", "foobarfoobar");
  }

  private void tesBinaryDuplicateHelper(StdTester tester, String input, String expectedOutput) {
    ByteBuffer argTest1 = ByteBuffer.wrap(input.getBytes());
    ByteBuffer expected = ByteBuffer.wrap(expectedOutput.getBytes());
    tester.check(functionCall("binary_duplicate", argTest1), expected, "varbinary");
  }
}
