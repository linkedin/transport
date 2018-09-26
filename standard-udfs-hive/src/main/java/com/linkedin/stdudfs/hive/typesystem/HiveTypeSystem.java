/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.hive.typesystem;

import com.linkedin.stdudfs.typesystem.AbstractTypeSystem;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;


public class HiveTypeSystem extends AbstractTypeSystem<ObjectInspector> {
  @Override
  protected ObjectInspector getArrayElementType(ObjectInspector dataType) {
    return ((ListObjectInspector) dataType).getListElementObjectInspector();
  }

  @Override
  protected ObjectInspector getMapKeyType(ObjectInspector dataType) {
    return ((MapObjectInspector) dataType).getMapKeyObjectInspector();
  }

  @Override
  protected ObjectInspector getMapValueType(ObjectInspector dataType) {
    return ((MapObjectInspector) dataType).getMapValueObjectInspector();
  }

  @Override
  protected List<ObjectInspector> getStructFieldTypes(ObjectInspector dataType) {
    return ((StructObjectInspector) dataType).getAllStructFieldRefs().stream().map(r -> r.getFieldObjectInspector()).
        collect(Collectors.toList());
  }

  @Override
  protected boolean isUnknownType(ObjectInspector dataType) {
    return dataType instanceof VoidObjectInspector;
  }

  @Override
  protected boolean isBooleanType(ObjectInspector dataType) {
    return dataType instanceof BooleanObjectInspector;
  }

  @Override
  protected boolean isIntegerType(ObjectInspector dataType) {
    return dataType instanceof IntObjectInspector;
  }

  @Override
  protected boolean isLongType(ObjectInspector dataType) {
    return dataType instanceof LongObjectInspector;
  }

  @Override
  protected boolean isStringType(ObjectInspector dataType) {
    return dataType instanceof StringObjectInspector;
  }

  @Override
  protected boolean isArrayType(ObjectInspector dataType) {
    return dataType instanceof ListObjectInspector;
  }

  @Override
  protected boolean isMapType(ObjectInspector dataType) {
    return dataType instanceof MapObjectInspector;
  }

  @Override
  protected boolean isStructType(ObjectInspector dataType) {
    return dataType instanceof StructObjectInspector;
  }

  @Override
  protected ObjectInspector createBooleanType() {
    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }

  @Override
  protected ObjectInspector createIntegerType() {
    return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  }

  @Override
  protected ObjectInspector createLongType() {
    return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
  }

  @Override
  protected ObjectInspector createStringType() {
    return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  }

  @Override
  protected ObjectInspector createUnknownType() {
    return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
  }

  @Override
  protected ObjectInspector createArrayType(ObjectInspector elementType) {
    return ObjectInspectorFactory.getStandardListObjectInspector(elementType);
  }

  @Override
  protected ObjectInspector createMapType(ObjectInspector keyType, ObjectInspector valueType) {
    return ObjectInspectorFactory.getStandardMapObjectInspector(
        keyType,
        valueType
    );
  }

  @Override
  protected ObjectInspector createStructType(List<String> fieldNames, List<ObjectInspector> fieldTypes) {
    if (fieldNames != null) {
      return ObjectInspectorFactory.getStandardStructObjectInspector(
          fieldNames, fieldTypes);
    } else {
      return ObjectInspectorFactory.getStandardStructObjectInspector(
          IntStream.range(0, fieldTypes.size()).mapToObj(i -> "field" + i).collect(Collectors.toList()),
          fieldTypes);
    }
  }
}
