/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.examples.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.testng.annotations.Test;

import static com.linkedin.stdudfs.hive.common.AssertHiveGenericUdf.*;
import static com.linkedin.stdudfs.hive.common.StandardObjectInspectorFactory.*;


public class TestMapFromTwoArraysFunctionWrapper {
  @Test
  public void testMapFromTwoArrays() {
    assertFunction(
        new MapFromTwoArraysFunctionWrapper(),
        new ObjectInspector[]{
            array(INTEGER),
            array(STRING)
        },
        new Object[]{
            ImmutableList.of(1, 2),
            ImmutableList.of("a", "b")
        },
        ImmutableMap.of(1, "a", 2, "b"));

    assertFunction(
        new MapFromTwoArraysFunctionWrapper(),
        new ObjectInspector[]{
            array(array(INTEGER)),
            array(array(STRING))
        },
        new Object[]{
            ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2)),
            ImmutableList.of(ImmutableList.of("a"), ImmutableList.of("b"))
        },
        ImmutableMap.of(ImmutableList.of(1), ImmutableList.of("a"), ImmutableList.of(2), ImmutableList.of("b")));

    assertFunction(
        new MapFromTwoArraysFunctionWrapper(),
        new ObjectInspector[]{
            array(array(INTEGER)),
            array(array(STRING))
        },
        new Object[]{
            null,
            ImmutableList.of(ImmutableList.of("a"), ImmutableList.of("b"))
        },
        null);

    assertFunction(
        new MapFromTwoArraysFunctionWrapper(),
        new ObjectInspector[]{
            array(array(INTEGER)),
            array(array(STRING))
        },
        new Object[]{
            ImmutableList.of(ImmutableList.of(1), ImmutableList.of(2)),
            null
        },
        null);
  }
}
