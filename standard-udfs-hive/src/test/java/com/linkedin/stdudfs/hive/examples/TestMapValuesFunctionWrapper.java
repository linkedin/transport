package com.linkedin.stdudfs.hive.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.testng.annotations.Test;

import static com.linkedin.stdudfs.hive.common.AssertHiveGenericUdf.*;
import static com.linkedin.stdudfs.hive.common.StandardObjectInspectorFactory.*;


public class TestMapValuesFunctionWrapper {
  @Test
  public void testMapValues() {
    assertFunction(
        new MapValuesFunctionWrapper(),
        new ObjectInspector[]{
            map(INTEGER, INTEGER)
        },
        new Object[]{
            ImmutableMap.of(1, 4, 2, 5, 3, 6)
        },
        ImmutableList.of(4, 5, 6));

    assertFunction(
        new MapValuesFunctionWrapper(),
        new ObjectInspector[]{
            map(STRING, STRING)
        },
        new Object[]{
            ImmutableMap.of("1", "4", "2", "5", "3", "6")
        },
        ImmutableList.of("4", "5", "6"));

    assertFunction(
        new MapValuesFunctionWrapper(),
        new ObjectInspector[]{
            map(STRING, STRING)
        },
        new Object[]{
            null
        },
        null);
  }
}
