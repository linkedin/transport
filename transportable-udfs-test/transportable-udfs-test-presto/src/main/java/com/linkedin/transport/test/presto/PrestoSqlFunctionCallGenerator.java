/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.presto;

import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.SqlFunctionCallGenerator;
import com.linkedin.transport.test.spi.types.TestType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class PrestoSqlFunctionCallGenerator implements SqlFunctionCallGenerator {

  @Override
  public String getFloatArgumentString(Float value) {
    return "REAL '" + value + "'";
  }

  @Override
  public String getLongArgumentString(Long value) {
    return "CAST(" + String.valueOf(value) + " AS BIGINT)";
  }

  @Override
  public String getStringArgumentString(String value) {
    return "CAST('" + String.valueOf(value) + "' AS VARCHAR)";
  }

  @Override
  public String getBinaryArgumentString(ByteBuffer value) {
    // Note that this does not work for PrestoSQL
    return "CAST('" + new String(value.array(), StandardCharsets.UTF_8) + "' AS VARBINARY)";
  }

  @Override
  public String getArrayArgumentString(List<Object> array, TestType arrayElementType) {
    return "ARRAY" + "[" + array.stream()
        .map(element -> getFunctionCallArgumentString(element, arrayElementType))
        .collect(Collectors.joining(", ")) + "]";
  }

  @Override
  public String getMapArgumentString(Map<Object, Object> map, TestType mapKeyType, TestType mapValueType) {
    return "map_from_entries(ARRAY[" + map.entrySet()
        .stream()
        .map(entry -> "(" + getFunctionCallArgumentString(entry.getKey(), mapKeyType) + ", "
            + getFunctionCallArgumentString(entry.getValue(), mapValueType) + ")")
        .collect(Collectors.joining(", ")) + "])";
  }

  @Override
  public String getStructArgumentString(Row struct, List<TestType> structFieldTypes) {
    List<Object> structFields = struct.getFields();
    return "(" + IntStream.range(0, structFields.size())
        .mapToObj(idx -> getFunctionCallArgumentString(structFields.get(idx), structFieldTypes.get(idx)))
        .collect(Collectors.joining(", ")) + ")";
  }
}
