/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi;

import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.test.spi.types.TestTypeUtils;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Represents a call to a function containing the function name and the parameters passed to it
 */
public class FunctionCall {

  private final String _functionName;
  private final List<Object> _parameters;
  private final List<TestType> _inferredParameterTypes;

  public FunctionCall(String functionName, List<Object> parameters) {
    _functionName = functionName;
    _parameters = parameters;
    _inferredParameterTypes = parameters.stream().map(TestTypeUtils::inferTypeFromData).collect(Collectors.toList());
  }

  public String getFunctionName() {
    return _functionName;
  }

  public List<Object> getParameters() {
    return _parameters;
  }

  public List<TestType> getInferredParameterTypes() {
    return _inferredParameterTypes;
  }
}
