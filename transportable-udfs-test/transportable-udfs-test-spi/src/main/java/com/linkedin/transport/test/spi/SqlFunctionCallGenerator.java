/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi;

import com.linkedin.transport.test.spi.types.ArrayTestType;
import com.linkedin.transport.test.spi.types.BooleanTestType;
import com.linkedin.transport.test.spi.types.IntegerTestType;
import com.linkedin.transport.test.spi.types.LongTestType;
import com.linkedin.transport.test.spi.types.MapTestType;
import com.linkedin.transport.test.spi.types.StringTestType;
import com.linkedin.transport.test.spi.types.StructTestType;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.test.spi.types.UnknownTestType;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * Creates a SQL function call string for the given function name and the function arguments
 */
public interface SqlFunctionCallGenerator {

  /**
   * Returns SQL function call string of the format {@code functionName(argument1, argument2, argument3, ...)}
   */
  default String getSqlFunctionCallString(FunctionCall functionCall) {
    return functionCall.getFunctionName() + "(" + IntStream.range(0, functionCall.getParameters().size())
        .mapToObj(idx -> getFunctionCallArgumentString(functionCall.getParameters().get(idx),
            functionCall.getInferredParameterTypes().get(idx)))
        .collect(Collectors.joining(", ")) + ")";
  }

  /**
   * Returns a SQL string representing the provided argument
   */
  default String getFunctionCallArgumentString(Object argument, TestType argumentType) {
    if (argument == null) {
      return getNullLiteralString();
    } else if (argument instanceof FunctionCall) {
      return getSqlFunctionCallString((FunctionCall) argument);
    } else if (argumentType instanceof UnknownTestType) {
      return getNullLiteralString();
    } else if (argumentType instanceof IntegerTestType) {
      return getIntegerArgumentString((Integer) argument);
    } else if (argumentType instanceof LongTestType) {
      return getLongArgumentString((Long) argument);
    } else if (argumentType instanceof BooleanTestType) {
      return getBooleanArgumentString((Boolean) argument);
    } else if (argumentType instanceof StringTestType) {
      return getStringArgumentString((String) argument);
    } else if (argumentType instanceof ArrayTestType) {
      return getArrayArgumentString((List<Object>) argument, ((ArrayTestType) argumentType).getElementType());
    } else if (argumentType instanceof MapTestType) {
      return getMapArgumentString((Map<Object, Object>) argument, ((MapTestType) argumentType).getKeyType(),
          ((MapTestType) argumentType).getValueType());
    } else if (argumentType instanceof StructTestType) {
      return getStructArgumentString((Row) argument, ((StructTestType) argumentType).getFieldTypes());
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + argumentType.getClass());
    }
  }

  default String getNullLiteralString() {
    return "NULL";
  }

  default String getIntegerArgumentString(Integer value) {
    return String.valueOf(value);
  }

  default String getLongArgumentString(Long value) {
    return String.valueOf(value) + "L";
  }

  default String getBooleanArgumentString(Boolean value) {
    return String.valueOf(value);
  }

  default String getStringArgumentString(String value) {
    return "'" + value + "'";
  }

  /**
   * Returns a SQL string of the format {@code ARRAY(ele1, ele2, ele3, ...)} representing an array literal
   */
  default String getArrayArgumentString(List<Object> array, TestType arrayElementType) {
    return "ARRAY" + "(" + array.stream()
        .map(element -> getFunctionCallArgumentString(element, arrayElementType))
        .collect(Collectors.joining(", ")) + ")";
  }

  /**
   * Returns a SQL string of the format {@code MAP((k1, v1), (k2, v2), (k3, v3), ...)} representing a map literal
   */
  default String getMapArgumentString(Map<Object, Object> map, TestType mapKeyType, TestType mapValueType) {
    return "MAP" + "(" + map.entrySet()
        .stream()
        .map(entry -> "(" + getFunctionCallArgumentString(entry.getKey(), mapKeyType) + ", "
            + getFunctionCallArgumentString(entry.getValue(), mapValueType) + ")")
        .collect(Collectors.joining(", ")) + ")";
  }

  /**
   * Returns a SQL string of the format {@code STRUCT(f1, f2, f3, ...)} representing a struct literal
   */
  default String getStructArgumentString(Row struct, List<TestType> structFieldTypes) {
    List<Object> structFields = struct.getFields();
    return "STRUCT" + "(" + IntStream.range(0, structFields.size())
        .mapToObj(idx -> getFunctionCallArgumentString(structFields.get(idx), structFieldTypes.get(idx)))
        .collect(Collectors.joining(", ")) + ")";
  }
}
