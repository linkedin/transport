/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.examples.hive;

import java.util.Arrays;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.testng.annotations.Test;

import static com.linkedin.transport.hive.common.AssertHiveGenericUdf.*;
import static com.linkedin.transport.hive.common.StandardObjectInspectorFactory.*;


public class TestArrayFillFunctionWrapper {
  @Test
  public void testArrayFill() {
    assertFunction(
        new ArrayFillFunctionWrapper(),
        new ObjectInspector[]{
            INTEGER,
            LONG
        },
        new Object[]{
            1,
            5L
        },
        Arrays.asList(1, 1, 1, 1, 1));

    assertFunction(
        new ArrayFillFunctionWrapper(),
        new ObjectInspector[]{
            STRING,
            LONG
        },
        new Object[]{
            "1",
            5L
        },
        Arrays.asList("1", "1", "1", "1", "1"));

    assertFunction(
        new ArrayFillFunctionWrapper(),
        new ObjectInspector[]{
            array(INTEGER),
            LONG
        },
        new Object[]{
            Arrays.asList(1),
            5L
        },
        Arrays.asList(Arrays.asList(1), Arrays.asList(1), Arrays.asList(1), Arrays.asList(1), Arrays.asList(1)));

    assertFunction(
        new ArrayFillFunctionWrapper(),
        new ObjectInspector[]{
            INTEGER,
            LONG
        },
        new Object[]{
            1,
            0L
        },
        Arrays.asList());

    assertFunction(
        new ArrayFillFunctionWrapper(),
        new ObjectInspector[]{
            INTEGER,
            LONG
        },
        new Object[]{
            1,
            null
        },
        null);

    assertFunction(
        new ArrayFillFunctionWrapper(),
        new ObjectInspector[]{
            INTEGER,
            LONG
        },
        new Object[]{
            null,
            5
        },
        null);
  }
}
