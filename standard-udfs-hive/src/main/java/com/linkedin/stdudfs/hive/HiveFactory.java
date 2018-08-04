package com.linkedin.stdudfs.hive;

import com.google.common.base.Preconditions;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdBoolean;
import com.linkedin.stdudfs.api.data.StdInteger;
import com.linkedin.stdudfs.api.data.StdLong;
import com.linkedin.stdudfs.api.data.StdMap;
import com.linkedin.stdudfs.api.data.StdString;
import com.linkedin.stdudfs.api.data.StdStruct;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.hive.data.HiveArray;
import com.linkedin.stdudfs.hive.data.HiveBoolean;
import com.linkedin.stdudfs.hive.data.HiveInteger;
import com.linkedin.stdudfs.hive.data.HiveLong;
import com.linkedin.stdudfs.hive.data.HiveMap;
import com.linkedin.stdudfs.hive.data.HiveString;
import com.linkedin.stdudfs.hive.data.HiveStruct;
import com.linkedin.stdudfs.hive.types.objectinspector.CacheableObjectInspectorConverters;
import com.linkedin.stdudfs.hive.typesystem.HiveTypeFactory;
import com.linkedin.stdudfs.typesystem.AbstractBoundVariables;
import com.linkedin.stdudfs.typesystem.TypeSignature;
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
