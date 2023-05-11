/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.typesystem;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.hive.types.HiveStructType;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHiveTypes {

  @Test
  public void testStructType() {
    StructObjectInspector structObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        ImmutableList.of("fieldOne", "fieldTwo"),
        ImmutableList.of(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector));
    HiveStructType hiveStructType = new HiveStructType(structObjectInspector);
    // Field names are case-insensitive
    Assert.assertEquals(hiveStructType.fieldNames(), ImmutableList.of("fieldone", "fieldtwo"));
    Assert.assertEquals(hiveStructType.fieldTypes().stream().map(StdType::underlyingType).collect(Collectors.toList()),
        ImmutableList.of(PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaDoubleObjectInspector));
  }
}
