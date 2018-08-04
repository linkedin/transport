package com.linkedin.stdudfs.hive.examples;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.testng.annotations.Test;

import static com.linkedin.stdudfs.hive.common.AssertHiveGenericUdf.*;
import static com.linkedin.stdudfs.hive.common.StandardObjectInspectorFactory.*;


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
