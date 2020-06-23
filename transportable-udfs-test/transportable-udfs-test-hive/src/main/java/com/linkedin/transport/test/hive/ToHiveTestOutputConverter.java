/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.hive;

import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.ToPlatformTestOutputConverter;
import com.linkedin.transport.test.spi.types.StringTestType;
import com.linkedin.transport.test.spi.types.TestType;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class ToHiveTestOutputConverter implements ToPlatformTestOutputConverter {

  /**
   * Returns string of the format [element1,element2,element3] for the given array
   */
  @Override
  public Object getArrayData(List<Object> array, TestType elementType) {
    return array.stream()
        .map(e -> getHiveTestOutputInComplexTypes(e, elementType))
        .collect(Collectors.joining(",", "[", "]"));
  }

  /**
   * Returns string of the format {key1:value1,key2:value2} for the given map
   */
  @Override
  public Object getMapData(Map<Object, Object> map, TestType mapKeyType, TestType mapValueType) {
    return map.entrySet()
        .stream()
        .map(x -> getHiveTestOutputInComplexTypes(x.getKey(), mapKeyType) + ":" + getHiveTestOutputInComplexTypes(
            x.getValue(), mapValueType))
        .collect(Collectors.joining(",", "{", "}"));
  }

  /**
   * Returns string of the format {"fieldName1":value2,"fieldName2":value2} for the given struct
   */
  @Override
  public Object getStructData(Row struct, List<TestType> fieldTypes, List<String> fieldNames) {
    return IntStream.range(0, struct.getFields().size())
        .mapToObj(idx -> "\"" + ((fieldNames != null) ? fieldNames.get(idx) : "field" + idx) + "\":"
            + getHiveTestOutputInComplexTypes(struct.getFields().get(idx), fieldTypes.get(idx)))
        .collect(Collectors.joining(",", "{", "}"));
  }

  @Override
  public Object getBinaryData(ByteBuffer value) {
    return value.array();
  }

  /**
   * In the output provided by {@link org.apache.hive.service.server.HiveServer2}, complex types are represented by
   * strings. So we need to return String values for primitives nested inside complex types.
   */
  private String getHiveTestOutputInComplexTypes(Object data, TestType dataType) {
    if (dataType instanceof StringTestType) {
      // for strings, enclose them in quotes if the data is not null
      if (data == null) {
        return "null";
      } else {
        return "\"" + convertToTestOutput(data, dataType) + "\"";
      }
    } else {
      // for all other types, return their strings representations
      return String.valueOf(convertToTestOutput(data, dataType));
    }
  }
}
