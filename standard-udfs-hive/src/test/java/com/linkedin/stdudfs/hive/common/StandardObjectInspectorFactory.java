package com.linkedin.stdudfs.hive.common;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class StandardObjectInspectorFactory {
  private StandardObjectInspectorFactory() {
  }

  final public static ObjectInspector INTEGER = PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  final public static ObjectInspector LONG = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
  final public static ObjectInspector BOOLEAN = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  final public static ObjectInspector STRING = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  final public static ObjectInspector NULL = PrimitiveObjectInspectorFactory.javaVoidObjectInspector;

  public static ObjectInspector array(ObjectInspector elementObjectInspector) {
    return ObjectInspectorFactory.getStandardListObjectInspector(elementObjectInspector);
  }

  public static ObjectInspector map(ObjectInspector keyObjectInspector, ObjectInspector valueObjectInspector) {
    return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
  }

  public static ObjectInspector struct(ObjectInspector... fieldObjectInspectors) {
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        IntStream.range(0, fieldObjectInspectors.length).mapToObj(i -> "f" + i).collect(Collectors.toList()),
        Arrays.asList(fieldObjectInspectors)
    );
  }
}
