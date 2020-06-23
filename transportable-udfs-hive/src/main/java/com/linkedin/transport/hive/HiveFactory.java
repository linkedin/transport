/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive;

import com.google.common.base.Preconditions;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdBoolean;
import com.linkedin.transport.api.data.StdBinary;
import com.linkedin.transport.api.data.StdDouble;
import com.linkedin.transport.api.data.StdFloat;
import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.data.StdLong;
import com.linkedin.transport.api.data.StdMap;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.data.StdStruct;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.hive.data.HiveArray;
import com.linkedin.transport.hive.data.HiveBoolean;
import com.linkedin.transport.hive.data.HiveBinary;
import com.linkedin.transport.hive.data.HiveDouble;
import com.linkedin.transport.hive.data.HiveFloat;
import com.linkedin.transport.hive.data.HiveInteger;
import com.linkedin.transport.hive.data.HiveLong;
import com.linkedin.transport.hive.data.HiveMap;
import com.linkedin.transport.hive.data.HiveString;
import com.linkedin.transport.hive.data.HiveStruct;
import com.linkedin.transport.hive.types.objectinspector.CacheableObjectInspectorConverters;
import com.linkedin.transport.hive.typesystem.HiveTypeFactory;
import com.linkedin.transport.typesystem.AbstractBoundVariables;
import com.linkedin.transport.typesystem.TypeSignature;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


public class HiveFactory implements StdFactory {

  final AbstractBoundVariables<ObjectInspector> _boundVariables;
  final CacheableObjectInspectorConverters _converters;
  final HiveTypeFactory _typeFactory;

  public HiveFactory(AbstractBoundVariables<ObjectInspector> boundVariables) {
    _boundVariables = boundVariables;
    _converters = new CacheableObjectInspectorConverters();
    _typeFactory = new HiveTypeFactory();
  }

  @Override
  public StdInteger createInteger(int value) {
    return new HiveInteger(value, PrimitiveObjectInspectorFactory.javaIntObjectInspector, this);
  }

  @Override
  public StdLong createLong(long value) {
    return new HiveLong(value, PrimitiveObjectInspectorFactory.javaLongObjectInspector, this);
  }

  @Override
  public StdBoolean createBoolean(boolean value) {
    return new HiveBoolean(value, PrimitiveObjectInspectorFactory.javaBooleanObjectInspector, this);
  }

  @Override
  public StdString createString(String value) {
    Preconditions.checkNotNull(value, "Cannot create a null StdString");
    return new HiveString(value, PrimitiveObjectInspectorFactory.javaStringObjectInspector, this);
  }

  @Override
  public StdFloat createFloat(float value) {
    return new HiveFloat(value, PrimitiveObjectInspectorFactory.javaFloatObjectInspector, this);
  }

  @Override
  public StdDouble createDouble(double value) {
    return new HiveDouble(value, PrimitiveObjectInspectorFactory.javaDoubleObjectInspector, this);
  }

  @Override
  public StdBinary createBinary(ByteBuffer value) {
    return new HiveBinary(value.array(), PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector, this);
  }

  @Override
  public StdArray createArray(StdType stdType, int expectedSize) {
    ListObjectInspector listObjectInspector = (ListObjectInspector) stdType.underlyingType();
    return new HiveArray(
        new ArrayList(expectedSize),
        ObjectInspectorFactory.getStandardListObjectInspector(listObjectInspector.getListElementObjectInspector()),
        this);
  }

  @Override
  public StdArray createArray(StdType stdType) {
    return createArray(stdType, 0);
  }

  @Override
  public StdMap createMap(StdType stdType) {
    MapObjectInspector mapObjectInspector = (MapObjectInspector) stdType.underlyingType();
    return new HiveMap(
        new HashMap(),
        ObjectInspectorFactory.getStandardMapObjectInspector(
            mapObjectInspector.getMapKeyObjectInspector(),
            mapObjectInspector.getMapValueObjectInspector()),
        this);
  }

  @Override
  public StdStruct createStruct(List<String> fieldNames, List<StdType> fieldTypes) {
    return new HiveStruct(
        new ArrayList(Arrays.asList(new Object[fieldTypes.size()])),
        ObjectInspectorFactory.getStandardStructObjectInspector(
            fieldNames,
            fieldTypes.stream().map(f -> (ObjectInspector) f.underlyingType()).collect(Collectors.toList())
        ),
        this);
  }

  @Override
  public StdStruct createStruct(List<StdType> fieldTypes) {
    List<String> fieldNames =
        IntStream.range(0, fieldTypes.size()).mapToObj(i -> "field" + i).collect(Collectors.toList());
    return createStruct(fieldNames, fieldTypes);
  }

  @Override
  public StdStruct createStruct(StdType stdType) {
    StructObjectInspector structObjectInspector = (StructObjectInspector) stdType.underlyingType();
    return new HiveStruct(
        new ArrayList(Arrays.asList(new Object[structObjectInspector.getAllStructFieldRefs().size()])),
        ObjectInspectorFactory.getStandardStructObjectInspector(
            structObjectInspector.getAllStructFieldRefs()
                .stream()
                .map(f -> f.getFieldName())
                .collect(Collectors.toList()),
            structObjectInspector.getAllStructFieldRefs()
                .stream()
                .map(f -> f.getFieldObjectInspector())
                .collect(Collectors.toList())
        ), this);
  }

  @Override
  public StdType createStdType(String typeSignature) {
    return HiveWrapper.createStdType(
        _typeFactory.createType(TypeSignature.parse(typeSignature), _boundVariables)
    );
  }

  public Converter getConverter(ObjectInspector inputOI, ObjectInspector outputOI) {
    return _converters.getConverter(inputOI, outputOI);
  }
}
