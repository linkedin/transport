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

// Temporarily disable the tests for Trino. As the test infrastructure from Trino named QueryAssertions is used to
// run these test for Trino, QueryAssertions mandatory execute the function with the query in two formats: one with
// is the normal query (e.g. SELECT "binary_duplicate"(a0) FROM (VALUES ROW(from_base64('YmFy'))) t(a0);), the other
// is with "where RAND()>0" clause (e.g. SELECT "binary_duplicate"(a0) FROM (VALUES ROW(from_base64('YmFy'))) t(a0) where RAND()>0;)
// QueryAssertions verifies the output from both queries are equal otherwise the test fail.
// However, the execution of the query with where clause triggers the code of VariableWidthBlockBuilder.writeByte() to create
// the input byte array in Slice with an initial 32 byes capacity, while the execution of the query without where clause does not trigger
// the code of VariableWidthBlockBuilder.writeByte() and create the input byte array in Slice with the actual capacity of the content.
// Therefore, the outputs from both queries are different.
public class TestBinaryDuplicateFunction extends AbstractStdUDFTest {
  @Override
  protected Map<Class<? extends TopLevelStdUDF>, List<Class<? extends StdUDF>>> getTopLevelStdUDFClassesAndImplementations() {
    return ImmutableMap.of(BinaryDuplicateFunction.class, ImmutableList.of(BinaryDuplicateFunction.class));
  }

  @Test
  public void testBinaryDuplicateASCII() {
    if (!Boolean.valueOf(System.getProperty("trinoTest"))) {
      StdTester tester = getTester();
      testBinaryDuplicateStringHelper(tester, "bar", "barbar");
      testBinaryDuplicateStringHelper(tester, "", "");
      testBinaryDuplicateStringHelper(tester, "foobar", "foobarfoobar");
    }
  }

  @Test
  public void testBinaryDuplicateUnicode() {
    if (!Boolean.valueOf(System.getProperty("trinoTest"))) {
      StdTester tester = getTester();
      testBinaryDuplicateStringHelper(tester, "こんにちは世界", "こんにちは世界こんにちは世界");
      testBinaryDuplicateStringHelper(tester, "\uD83D\uDE02", "\uD83D\uDE02\uD83D\uDE02");
    }
  }

  private void testBinaryDuplicateStringHelper(StdTester tester, String input, String expectedOutput) {
    ByteBuffer inputBuffer = ByteBuffer.wrap(input.getBytes());
    ByteBuffer expected = ByteBuffer.wrap(expectedOutput.getBytes());
    tester.check(functionCall("binary_duplicate", inputBuffer), expected, "varbinary");
  }

  @Test
  public void testBinaryDuplicate() {
    if (!Boolean.valueOf(System.getProperty("trinoTest"))) {
      StdTester tester = getTester();
      testBinaryDuplicateHelper(tester, new byte[]{1, 2, 3}, new byte[]{1, 2, 3, 1, 2, 3});
      testBinaryDuplicateHelper(tester, new byte[]{-1, -2, -3}, new byte[]{-1, -2, -3, -1, -2, -3});
    }
  }

  private void testBinaryDuplicateHelper(StdTester tester, byte[] input, byte[] expectedOutput) {
    ByteBuffer inputBuffer = ByteBuffer.wrap(input);
    ByteBuffer expected = ByteBuffer.wrap(expectedOutput);
    tester.check(functionCall("binary_duplicate", inputBuffer), expected, "varbinary");
  }
}
