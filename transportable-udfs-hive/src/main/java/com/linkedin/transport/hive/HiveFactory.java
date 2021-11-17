/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive;

import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.hive.data.HiveArrayData;
import com.linkedin.transport.hive.data.HiveMapData;
import com.linkedin.transport.hive.data.HiveRowData;
import com.linkedin.transport.hive.types.objectinspector.CacheableObjectInspectorConverters;
import com.linkedin.transport.hive.typesystem.HiveTypeFactory;
import com.linkedin.transport.typesystem.AbstractBoundVariables;
import com.linkedin.transport.typesystem.TypeSignature;
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


public class HiveFactory implements TypeFactory {

  final AbstractBoundVariables<ObjectInspector> _boundVariables;
  final CacheableObjectInspectorConverters _converters;
  final HiveTypeFactory _typeFactory;

  public HiveFactory(AbstractBoundVariables<ObjectInspector> boundVariables) {
    _boundVariables = boundVariables;
    _converters = new CacheableObjectInspectorConverters();
    _typeFactory = new HiveTypeFactory();
  }

  @Override
  public ArrayData createArray(DataType dataType, int expectedSize) {
    ListObjectInspector listObjectInspector = (ListObjectInspector) dataType.underlyingType();
    return new HiveArrayData(
        new ArrayList(expectedSize),
        ObjectInspectorFactory.getStandardListObjectInspector(listObjectInspector.getListElementObjectInspector()),
        this);
  }

  @Override
  public ArrayData createArray(DataType dataType) {
    return createArray(dataType, 0);
  }

  @Override
  public MapData createMap(DataType dataType) {
    MapObjectInspector mapObjectInspector = (MapObjectInspector) dataType.underlyingType();
    return new HiveMapData(
        new HashMap(),
        ObjectInspectorFactory.getStandardMapObjectInspector(
            mapObjectInspector.getMapKeyObjectInspector(),
            mapObjectInspector.getMapValueObjectInspector()),
        this);
  }

  @Override
  public RowData createStruct(List<String> fieldNames, List<DataType> fieldTypes) {
    return new HiveRowData(
        new ArrayList(Arrays.asList(new Object[fieldTypes.size()])),
        ObjectInspectorFactory.getStandardStructObjectInspector(
            fieldNames,
            fieldTypes.stream().map(f -> (ObjectInspector) f.underlyingType()).collect(Collectors.toList())
        ),
        this);
  }

  @Override
  public RowData createStruct(List<DataType> fieldTypes) {
    List<String> fieldNames =
        IntStream.range(0, fieldTypes.size()).mapToObj(i -> "field" + i).collect(Collectors.toList());
    return createStruct(fieldNames, fieldTypes);
  }

  @Override
  public RowData createStruct(DataType dataType) {
    StructObjectInspector structObjectInspector = (StructObjectInspector) dataType.underlyingType();
    return new HiveRowData(
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
  public DataType createDataType(String typeSignature) {
    return HiveWrapper.createStdType(
        _typeFactory.createType(TypeSignature.parse(typeSignature), _boundVariables)
    );
  }

  public Converter getConverter(ObjectInspector inputOI, ObjectInspector outputOI) {
    return _converters.getConverter(inputOI, outputOI);
  }
}
