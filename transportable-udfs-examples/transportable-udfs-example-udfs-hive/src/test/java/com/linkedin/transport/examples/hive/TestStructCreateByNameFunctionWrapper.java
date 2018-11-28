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


public class TestStructCreateByNameFunctionWrapper {
  @Test
  public void testStructCreateByName() {
    assertFunction(
        new StructCreateByNameFunctionWrapper(),
        new ObjectInspector[]{
            STRING,
            STRING,
            STRING,
            STRING
        },
        new Object[]{"a", "x", "b", "y"},
        ImmutableList.of("x", "y")
    );

    assertFunction(
        new StructCreateByNameFunctionWrapper(),
        new ObjectInspector[]{
            STRING,
            STRING,
            STRING,
            STRING
        },
        new Object[]{null, "x", "b", "y"},
        null
    );

    assertFunction(
        new StructCreateByNameFunctionWrapper(),
        new ObjectInspector[]{
            STRING,
            STRING,
            STRING,
            STRING
        },
        new Object[]{"a", null, "b", "y"},
        null
    );

    assertFunction(
        new StructCreateByNameFunctionWrapper(),
        new ObjectInspector[]{
            STRING,
            STRING,
            STRING,
            STRING
        },
        new Object[]{"a", "x", null, "y"},
        null
    );

    assertFunction(
        new StructCreateByNameFunctionWrapper(),
        new ObjectInspector[]{
            STRING,
            STRING,
            STRING,
            STRING
        },
        new Object[]{"a", "x", "b", null},
        null
    );
  }
}
