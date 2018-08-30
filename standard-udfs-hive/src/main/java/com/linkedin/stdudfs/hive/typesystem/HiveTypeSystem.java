/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
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
