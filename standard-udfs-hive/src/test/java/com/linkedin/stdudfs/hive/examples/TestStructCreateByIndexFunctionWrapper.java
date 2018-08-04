package com.linkedin.stdudfs.hive.examples;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.testng.annotations.Test;

import static com.linkedin.stdudfs.hive.common.AssertHiveGenericUdf.*;
import static com.linkedin.stdudfs.hive.common.StandardObjectInspectorFactory.*;


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
