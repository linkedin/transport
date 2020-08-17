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
  public void testBinaryDuplicateASCII() {
    StdTester tester = getTester();
    testBinaryDuplicateStringHelper(tester, "bar", "barbar");
    testBinaryDuplicateStringHelper(tester, "", "");
    testBinaryDuplicateStringHelper(tester, "foobar", "foobarfoobar");
  }

  @Test
  public void testBinaryDuplicateUnicode() {
    StdTester tester = getTester();
    testBinaryDuplicateStringHelper(tester, "こんにちは世界", "こんにちは世界こんにちは世界");
    testBinaryDuplicateStringHelper(tester, "\uD83D\uDE02", "\uD83D\uDE02\uD83D\uDE02");
  }

  private void testBinaryDuplicateStringHelper(StdTester tester, String input, String expectedOutput) {
    ByteBuffer inputBuffer = ByteBuffer.wrap(input.getBytes());
    ByteBuffer expected = ByteBuffer.wrap(expectedOutput.getBytes());
    tester.check(functionCall("binary_duplicate", inputBuffer), expected, "varbinary");
  }

  @Test
  public void testBinaryDuplicate() {
    StdTester tester = getTester();
    testBinaryDuplicateHelper(tester, new byte[] {1, 2, 3}, new byte[] {1, 2, 3, 1, 2, 3});
    testBinaryDuplicateHelper(tester, new byte[] {-1, -2, -3}, new byte[] {-1, -2, -3, -1, -2, -3});
  }

  private void testBinaryDuplicateHelper(StdTester tester, byte[] input, byte[] expectedOutput) {
    ByteBuffer inputBuffer = ByteBuffer.wrap(input);
    ByteBuffer expected = ByteBuffer.wrap(expectedOutput);
    tester.check(functionCall("binary_duplicate", inputBuffer), expected, "varbinary");
  }
}
