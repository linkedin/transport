/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.hive;

import com.linkedin.transport.test.spi.SqlFunctionCallGenerator;
import com.linkedin.transport.test.spi.types.TestType;
import java.util.Map;
import java.util.stream.Collectors;


public class HiveSqlFunctionCallGenerator implements SqlFunctionCallGenerator {

  @Override
  public String getMapArgumentString(Map<Object, Object> map, TestType mapKeyType, TestType mapValueType) {
    return "map_from_entries(ARRAY(" + map.entrySet()
        .stream()
        .map(entry -> "STRUCT(" + getFunctionCallArgumentString(entry.getKey(), mapKeyType) + ", "
            + getFunctionCallArgumentString(entry.getValue(), mapValueType) + ")")
        .collect(Collectors.joining(", ")) + "))";
  }
}
