/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.trino;

import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.ToPlatformTestOutputConverter;
import com.linkedin.transport.test.spi.types.TestType;
import io.trino.spi.type.SqlVarbinary;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class ToTrinoTestOutputConverter implements ToPlatformTestOutputConverter {

  /**
   * Returns a {@link List} for the given array while also converting nested elements
   */
  @Override
  public Object getArrayData(List<Object> array, TestType elementType) {
    return array.stream().map(e -> convertToTestOutput(e, elementType)).collect(Collectors.toList());
  }

  /**
   * Returns a {@link Map} for the given map while also converting nested elements
   */
  @Override
  public Object getMapData(Map<Object, Object> map, TestType mapKeyType, TestType mapValueType) {
    Map<Object, Object> result = new HashMap<>();
    for (Map.Entry<Object, Object> e : map.entrySet()) {
      result.put(convertToTestOutput(e.getKey(), mapKeyType), convertToTestOutput(e.getValue(), mapValueType));
    }
    return result;
  }

  /**
   * Returns a {@link List} for the given struct while also converting nested elements
   */
  @Override
  public Object getStructData(Row struct, List<TestType> fieldTypes, List<String> fieldNames) {
    return IntStream.range(0, struct.getFields().size())
        .mapToObj(idx -> convertToTestOutput(struct.getFields().get(idx), fieldTypes.get(idx)))
        .collect(Collectors.toList());
  }

  @Override
  public Object getBinaryData(ByteBuffer value) {
    return new SqlVarbinary(value.array());
  }
}
