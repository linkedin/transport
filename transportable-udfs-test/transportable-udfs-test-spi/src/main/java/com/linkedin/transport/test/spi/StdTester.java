/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi;

import com.linkedin.transport.api.udf.UDF;
import com.linkedin.transport.api.udf.TopLevelUDF;
import java.util.List;
import java.util.Map;


/**
 * An interface providing testing capabilities for UDFs to be implemented by
 * each target platform.
 */
public interface StdTester {

  /**
   * Registers UDFs the user wants to test with the underlying test framework
   */
  void setup(Map<Class<? extends TopLevelUDF>, List<Class<? extends UDF>>> topLevelStdUDFClassesAndImplementations);

  /**
   * Verifies that the output and output type of the function call matches the provided expected values in the {@link TestCase}
   */
  void check(TestCase testCase);

  /**
   * Verifies that the output and output type of the function call matches the provided expected values
   *
   * @param functionCall  A {@link FunctionCall} with function name and a list of input arguments.
   * @param expectedOutput  The expected output data
   * @param expectedOutputTypeSignature  The type signature string for the data type of the expected output
   */
  default void check(FunctionCall functionCall, Object expectedOutput, String expectedOutputTypeSignature) {
    check(new TestCase(functionCall, expectedOutput, expectedOutputTypeSignature));
  }
}
