/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic;

import com.linkedin.transport.test.spi.FunctionCall;
import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.types.ArrayTestType;
import com.linkedin.transport.test.spi.types.BooleanTestType;
import com.linkedin.transport.test.spi.types.IntegerTestType;
import com.linkedin.transport.test.spi.types.LongTestType;
import com.linkedin.transport.test.spi.types.MapTestType;
import com.linkedin.transport.test.spi.types.StringTestType;
import com.linkedin.transport.test.spi.types.StructTestType;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.test.spi.types.TestTypeFactory;
import com.linkedin.transport.test.spi.types.TestTypeUtils;
import com.linkedin.transport.test.spi.types.UnknownTestType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.commons.lang3.tuple.Pair;


class GenericQueryExecutor {

  private final Map<String, GenericStdUDFWrapper> _functionNameToWrapperMap;

  GenericQueryExecutor(Map<String, GenericStdUDFWrapper> functionNameToWrapperMap) {
    _functionNameToWrapperMap = functionNameToWrapperMap;
  }

  Pair<TestType, Object> executeQuery(FunctionCall functionCall) {
    return resolveFunctionCall(functionCall);
  }

  private Pair<TestType, Object> resolveFunctionCall(FunctionCall call) {
    GenericStdUDFWrapper wrapper = _functionNameToWrapperMap.get(call.getFunctionName());
    if (wrapper == null) {
      throw new RuntimeException("Could not find UDF with name " + call.getFunctionName());
    }

    List<TestType> resolvedParameterTypes = new ArrayList<>();
    List<Object> resolvedParameters = new ArrayList<>();
    // If any of the parameters has a function call, resolve the type and value of the nested function call first
    IntStream.range(0, call.getInferredParameterTypes().size()).forEach(idx -> {
      Pair<TestType, Object> result =
          resolveParameter(call.getParameters().get(idx), call.getInferredParameterTypes().get(idx));
      resolvedParameterTypes.add(result.getLeft());
      resolvedParameters.add(result.getRight());
    });

    TestType outputType = wrapper.initialize(resolvedParameterTypes.toArray(new TestType[0]));
    Object result = wrapper.evaluate(resolvedParameters.toArray(new Object[0]));
    return Pair.of(outputType, result);
  }

  private Pair<TestType, Object> resolveParameter(Object argument, TestType argumentType) {
    if (argument instanceof FunctionCall) {
      return resolveFunctionCall((FunctionCall) argument);
    } else if (argument == null || argumentType instanceof UnknownTestType || argumentType instanceof IntegerTestType
        || argumentType instanceof LongTestType || argumentType instanceof BooleanTestType
        || argumentType instanceof StringTestType) {
      return Pair.of(argumentType, argument);
    } else if (argumentType instanceof ArrayTestType) {
      return resolveArray((List<Object>) argument, ((ArrayTestType) argumentType).getElementType());
    } else if (argumentType instanceof MapTestType) {
      return resolveMap((Map<Object, Object>) argument, ((MapTestType) argumentType).getKeyType(),
          ((MapTestType) argumentType).getValueType());
    } else if (argumentType instanceof StructTestType) {
      return resolveStruct((Row) argument, ((StructTestType) argumentType).getFieldTypes());
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + argumentType.getClass());
    }
  }

  private Pair<TestType, Object> resolveArray(List<Object> array, TestType elementType) {
    List<TestType> resolvedElementTypes = new ArrayList<>();
    List<Object> resolvedArray = new ArrayList<>();
    array.forEach(element -> {
      Pair<TestType, Object> resolvedElement = resolveParameter(element, elementType);
      resolvedElementTypes.add(resolvedElement.getLeft());
      resolvedArray.add(resolvedElement.getRight());
    });
    return Pair.of(TestTypeFactory.array(TestTypeUtils.inferCollectionType(resolvedElementTypes, "array elements")),
        resolvedArray);
  }

  private Pair<TestType, Object> resolveMap(Map<Object, Object> map, TestType keyType, TestType valueType) {
    List<TestType> resolvedKeyTypes = new ArrayList<>();
    List<TestType> resolvedValueTypes = new ArrayList<>();
    Map<Object, Object> resolvedMap = new LinkedHashMap<>();
    map.forEach((key, value) -> {
      Pair<TestType, Object> resolvedKey = resolveParameter(key, keyType);
      Pair<TestType, Object> resolvedValue = resolveParameter(value, valueType);
      resolvedKeyTypes.add(resolvedKey.getLeft());
      resolvedValueTypes.add(resolvedValue.getLeft());
      resolvedMap.put(resolvedKey.getRight(), resolvedValue.getRight());
    });
    return Pair.of(TestTypeFactory.map(TestTypeUtils.inferCollectionType(resolvedKeyTypes, "map keys"),
        TestTypeUtils.inferCollectionType(resolvedValueTypes, "map values")), resolvedMap);
  }

  private Pair<TestType, Object> resolveStruct(Row struct, List<TestType> fieldTypes) {
    List<TestType> resolvedFieldTypes = new ArrayList<>();
    List<Object> resolvedFields = new ArrayList<>();
    IntStream.range(0, fieldTypes.size()).forEach(idx -> {
      Pair<TestType, Object> resolvedField = resolveParameter(struct.getFields().get(idx), fieldTypes.get(idx));
      resolvedFieldTypes.add(resolvedField.getLeft());
      resolvedFields.add(resolvedField.getRight());
    });
    return Pair.of(TestTypeFactory.struct(resolvedFieldTypes), new Row(resolvedFields));
  }
}
