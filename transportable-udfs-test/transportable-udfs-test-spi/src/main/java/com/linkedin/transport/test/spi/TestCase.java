/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi;

import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.test.spi.types.TestTypeUtils;


/**
 * Represents a test case to be tested, viz. the function name, inputs, the expected output and the data type of the expected output
 */
public class TestCase {

  private FunctionCall _functionCall;
  private Object _expectedOutput;
  private TestType _inferredOutputType;
  private String _expectedOutputType;

  public TestCase(FunctionCall functionCall, Object expectedOutput, String expectedOutputType) {
    _functionCall = functionCall;
    _expectedOutput = expectedOutput;
    _inferredOutputType = TestTypeUtils.inferTypeFromData(expectedOutput);
    _expectedOutputType = expectedOutputType;
  }

  public FunctionCall getFunctionCall() {
    return _functionCall;
  }

  public Object getExpectedOutput() {
    return _expectedOutput;
  }

  public TestType getInferredOutputType() {
    return _inferredOutputType;
  }

  public String getExpectedOutputType() {
    return _expectedOutputType;
  }
}
