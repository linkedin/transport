/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.hive;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.testng.annotations.Test;

import static com.linkedin.transport.hive.common.AssertHiveGenericUdf.*;
import static com.linkedin.transport.hive.common.StandardObjectInspectorFactory.*;


public class TestStructCreateByIndexFunctionWrapper {
  @Test
  public void testStructCreateByIndex() {
    assertFunction(
        new StructCreateByIndexFunctionWrapper(),
        new ObjectInspector[]{
            STRING,
            STRING
        },
        new Object[]{"x", "y"},
        ImmutableList.of("x", "y")
    );

    assertFunction(
        new StructCreateByIndexFunctionWrapper(),
        new ObjectInspector[]{
            STRING,
            STRING
        },
        new Object[]{null, "y"},
        null
    );

    assertFunction(
        new StructCreateByIndexFunctionWrapper(),
        new ObjectInspector[]{
            STRING,
            STRING
        },
        new Object[]{"x", null},
        null
    );
  }
}
