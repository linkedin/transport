/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi.types;

import com.linkedin.transport.test.spi.FunctionCall;
import com.linkedin.transport.test.spi.Row;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class TestTypeUtils {

  private TestTypeUtils() {
  }

  public static TestType inferTypeFromData(Object data) {
    if (data == null) {
      return TestTypeFactory.UNKNOWN_TEST_TYPE;
    } else if (data instanceof Integer) {
      return TestTypeFactory.INTEGER_TEST_TYPE;
    } else if (data instanceof Long) {
      return TestTypeFactory.LONG_TEST_TYPE;
    } else if (data instanceof Boolean) {
      return TestTypeFactory.BOOLEAN_TEST_TYPE;
    } else if (data instanceof String) {
      return TestTypeFactory.STRING_TEST_TYPE;
    } else if (data instanceof Float) {
      return TestTypeFactory.FLOAT_TEST_TYPE;
    } else if (data instanceof Double) {
      return TestTypeFactory.DOUBLE_TEST_TYPE;
    } else if (data instanceof ByteBuffer) {
      return TestTypeFactory.BINARY_TEST_TYPE;
    } else if (data instanceof List) {
      return TestTypeFactory.array(inferCollectionTypeFromData((List) data, "array elements"));
    } else if (data instanceof Map) {
      Map map = (Map) data;
      return TestTypeFactory.map(inferCollectionTypeFromData(map.keySet(), "map keys"),
          inferCollectionTypeFromData(map.values(), "map values"));
    } else if (data instanceof Row) {
      return TestTypeFactory.struct(
          ((Row) data).getFields().stream().map(TestTypeUtils::inferTypeFromData).collect(Collectors.toList()));
    } else if (data instanceof FunctionCall) {
      return TestTypeFactory.UNKNOWN_TEST_TYPE;
    } else {
      throw new IllegalArgumentException("Input parameter of type " + data.getClass() + " not supported.");
    }
  }

  private static TestType inferCollectionTypeFromData(Collection<Object> data, String elementDescription) {
    return inferCollectionType(data.stream().map(TestTypeUtils::inferTypeFromData).collect(Collectors.toList()),
        elementDescription);
  }

  public static TestType inferCollectionType(Collection<TestType> testTypes, String elementDescription) {
    TestType expectedElementType = TestTypeFactory.UNKNOWN_TEST_TYPE;
    for (TestType testType : testTypes) {
      // if expected type is unknown (nulls or function call), override with the current type
      if (expectedElementType == TestTypeFactory.UNKNOWN_TEST_TYPE) {
        expectedElementType = testType;
      } else if (testType != TestTypeFactory.UNKNOWN_TEST_TYPE && expectedElementType != testType
          && !expectedElementType.equals(testType)) {
        // if expected type is known, allow nulls (unknown type), function calls (output type cannot be resolved at this
        // moment) or the expected type
        throw new IllegalArgumentException(
            "All " + elementDescription + " must be of the same type. Expected: " + expectedElementType + " Actual: "
                + testType);
      }
    }
    return expectedElementType;
  }
}
