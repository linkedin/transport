/**
 * Copyright 2023 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive.data;

import com.google.common.collect.ImmutableList;
import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.hive.HiveFactory;
import com.linkedin.transport.hive.typesystem.HiveBoundVariables;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestHiveStruct {

  @Test
  public void testSetFieldOnObjectArray() {
    StructObjectInspector structObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        ImmutableList.of("field0", "field1"),
        ImmutableList.of(
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
        )
    );
    Object[] array = new Object[]{10, "hello"};
    HiveFactory factory = new HiveFactory(new HiveBoundVariables());
    HiveStruct struct = new HiveStruct(array, structObjectInspector, factory);

    Assert.assertEquals(((StdInteger) struct.getField(0)).get(), 10);
    Assert.assertEquals(((StdString) struct.getField(1)).get(), "hello");
    Assert.assertEquals(((StdInteger) struct.getField("field0")).get(), 10);

    // Modify via index
    struct.setField(0, factory.createInteger(20));
    Assert.assertEquals(array[0], 20);
    Assert.assertEquals(((StdInteger) struct.getField(0)).get(), 20);

    // Modify via field name
    struct.setField("field1", factory.createString("world"));
    Assert.assertEquals(array[1], "world");
    Assert.assertEquals(((StdString) struct.getField(1)).get(), "world");
  }

  @Test
  public void testSetFieldOnObjectArrayWithWritableInspectors() {
    StructObjectInspector structObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        ImmutableList.of("field0", "field1"),
        ImmutableList.of(
            PrimitiveObjectInspectorFactory.writableIntObjectInspector,
            PrimitiveObjectInspectorFactory.writableStringObjectInspector
        )
    );
    Object[] array = new Object[]{new IntWritable(1), new Text("initial")};
    HiveFactory factory = new HiveFactory(new HiveBoundVariables());
    HiveStruct struct = new HiveStruct(array, structObjectInspector, factory);

    struct.setField(0, factory.createInteger(42));
    Assert.assertTrue(array[0] instanceof IntWritable);
    Assert.assertEquals(((IntWritable) array[0]).get(), 42);

    struct.setField("field1", factory.createString("updated"));
    Assert.assertTrue(array[1] instanceof Text);
    Assert.assertEquals(((Text) array[1]).toString(), "updated");
  }

  @Test
  public void testSetFieldOnList() {
    StructObjectInspector structObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        ImmutableList.of("field0", "field1"),
        ImmutableList.of(
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector
        )
    );
    List<Object> list = new ArrayList<>(Arrays.asList(1, "abc"));
    HiveFactory factory = new HiveFactory(new HiveBoundVariables());
    HiveStruct struct = new HiveStruct(list, structObjectInspector, factory);

    struct.setField(0, factory.createInteger(99));
    Assert.assertEquals(list.get(0), 99);
    Assert.assertEquals(((StdInteger) struct.getField(0)).get(), 99);

    struct.setField("field1", factory.createString("xyz"));
    Assert.assertEquals(list.get(1), "xyz");
    Assert.assertEquals(((StdString) struct.getField(1)).get(), "xyz");
  }
}
