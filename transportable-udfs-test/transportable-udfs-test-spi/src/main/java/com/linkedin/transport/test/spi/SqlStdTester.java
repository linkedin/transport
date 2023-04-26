/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi;

import com.linkedin.transport.api.StdFactory;


/**
 * A {@link StdTester} for platforms exposing a SQL-based testing interface
 */
public interface SqlStdTester extends StdTester {

  StdFactory getStdFactory();

  SqlFunctionCallGenerator getSqlFunctionCallGenerator();

  ToPlatformTestOutputConverter getToPlatformTestOutputConverter();

  /**
   * Executes the SQL function call against the platform and asserts the output data and type
   * @param functionCallString  The SQL function call query string
   * @param expectedOutputData  The expected output data from the function call
   * @param expectedOutputType  The expected output type from the function call
   */
  default void assertFunctionCall(String functionCallString, Object expectedOutputData, Object expectedOutputType) {
    throw new UnsupportedOperationException();
  }

  default void check(TestCase testCase) {
    assertFunctionCall(getSqlFunctionCallGenerator().getSqlFunctionCallString(testCase.getFunctionCall()),
        getToPlatformTestOutputConverter().convertToTestOutput(testCase.getExpectedOutput(),
            testCase.getInferredOutputType()), getPlatformType(testCase.getExpectedOutputType()));
  }

  default Object getPlatformType(String typeSignature) {
    return getStdFactory().createStdType(typeSignature).underlyingType();
  }
}
