/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.hive.types.objectinspector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;


/**
 * CacheableObjectInspectorConverters.
 *
 */
public final class CacheableObjectInspectorConverters {

  private Map<Pair<ObjectInspector, ObjectInspector>, Converter> _cachedConverters;

  public CacheableObjectInspectorConverters() {
    _cachedConverters = new ConcurrentHashMap<>();
  }

  private Converter getConverterFromCache(ObjectInspector inputOI, ObjectInspector outputOI) {
    return _cachedConverters.get(Pair.of(inputOI, outputOI));
  }

  private void cacheConverter(ObjectInspector inputOI, ObjectInspector outputOI, Converter c) {
    _cachedConverters.putIfAbsent(Pair.of(inputOI, outputOI), c);
  }

  private Converter getConverter(PrimitiveObjectInspector inputOI, PrimitiveObjectInspector outputOI) {
    Converter c = getConverterFromCache(inputOI, outputOI);
    if (c != null) {
      return c;
    }
    switch (outputOI.getPrimitiveCategory()) {
      case BOOLEAN:
        c = new CacheablePrimitiveObjectInspectorConverter.BooleanConverter(inputOI,
            (SettableBooleanObjectInspector) outputOI);
        break;
      case BYTE:
        c = new CacheablePrimitiveObjectInspectorConverter.ByteConverter(inputOI,
            (SettableByteObjectInspector) outputOI);
        break;
      case SHORT:
        c = new CacheablePrimitiveObjectInspectorConverter.ShortConverter(inputOI,
            (SettableShortObjectInspector) outputOI);
        break;
      case INT:
        c = new CacheablePrimitiveObjectInspectorConverter.IntConverter(inputOI, (SettableIntObjectInspector) outputOI);
        break;
      case LONG:
        c = new CacheablePrimitiveObjectInspectorConverter.LongConverter(inputOI,
            (SettableLongObjectInspector) outputOI);
        break;
      case FLOAT:
        c = new CacheablePrimitiveObjectInspectorConverter.FloatConverter(inputOI,
            (SettableFloatObjectInspector) outputOI);
        break;
      case DOUBLE:
        c = new CacheablePrimitiveObjectInspectorConverter.DoubleConverter(inputOI,
            (SettableDoubleObjectInspector) outputOI);
        break;
      case STRING:
        if (outputOI instanceof WritableStringObjectInspector) {
          c = new CacheablePrimitiveObjectInspectorConverter.TextConverter(inputOI);
        } else if (outputOI instanceof JavaStringObjectInspector) {
          c = new CacheablePrimitiveObjectInspectorConverter.StringConverter(inputOI);
        }
        break;
      case CHAR:
        c = new CacheablePrimitiveObjectInspectorConverter.HiveCharConverter(inputOI,
            (SettableHiveCharObjectInspector) outputOI);
        break;
      case VARCHAR:
        c = new CacheablePrimitiveObjectInspectorConverter.HiveVarcharConverter(inputOI,
            (SettableHiveVarcharObjectInspector) outputOI);
        break;
      case DATE:
        c = new CacheablePrimitiveObjectInspectorConverter.DateConverter(inputOI,
            (SettableDateObjectInspector) outputOI);
        break;
      case TIMESTAMP:
        c = new CacheablePrimitiveObjectInspectorConverter.TimestampConverter(inputOI,
            (SettableTimestampObjectInspector) outputOI);
        break;
      case BINARY:
        c = new CacheablePrimitiveObjectInspectorConverter.BinaryConverter(inputOI,
            (SettableBinaryObjectInspector) outputOI);
        break;
      case DECIMAL:
        c = new CacheablePrimitiveObjectInspectorConverter.HiveDecimalConverter(inputOI,
            (SettableHiveDecimalObjectInspector) outputOI);
        break;
      default:
        throw new UnsupportedOperationException(
            "Hive internal error: conversion of " + inputOI.getTypeName() + " to " + outputOI.getTypeName()
                + " not supported yet.");
    }
    cacheConverter(inputOI, outputOI, c);
    return c;
  }

  /**
   * Returns a converter that converts objects from one OI to another OI. The
   * returned (converted) object does not belong to the converter. Hence once convertor can be used
   * multiple times within one eval invocation.
   */
  public Converter getConverter(ObjectInspector inputOI, ObjectInspector outputOI) {
    // If the inputOI is the same as the outputOI, just return an
    // IdentityConverter.
    if (inputOI.equals(outputOI)) {
      return new ObjectInspectorConverters.IdentityConverter();
    }
    Converter c = getConverterFromCache(inputOI, outputOI);
    if (c != null) {
      return c;
    }
    switch (outputOI.getCategory()) {
      case PRIMITIVE:
        return getConverter((PrimitiveObjectInspector) inputOI, (PrimitiveObjectInspector) outputOI);
      case STRUCT:
        c = new StructConverter(inputOI, (SettableStructObjectInspector) outputOI);
        break;
      case LIST:
        c = new ListConverter(inputOI, (SettableListObjectInspector) outputOI);
        break;
      case MAP:
        c = new MapConverter(inputOI, (SettableMapObjectInspector) outputOI);
        break;
      default:
        throw new UnsupportedOperationException(
            "Hive internal error: conversion of " + inputOI.getTypeName() + " to " + outputOI.getTypeName()
                + " not supported yet.");
    }
    cacheConverter(inputOI, outputOI, c);
    return c;
  }

  /**
   * A converter class for List.
   */
  public class ListConverter implements Converter {

    ListObjectInspector inputOI;
    SettableListObjectInspector outputOI;

    ObjectInspector inputElementOI;
    ObjectInspector outputElementOI;

    Converter elementConverter;

    public ListConverter(ObjectInspector inputOI, SettableListObjectInspector outputOI) {
      if (inputOI instanceof ListObjectInspector) {
        this.inputOI = (ListObjectInspector) inputOI;
        this.outputOI = outputOI;
        inputElementOI = this.inputOI.getListElementObjectInspector();
        outputElementOI = outputOI.getListElementObjectInspector();
        elementConverter = getConverter(inputElementOI, outputElementOI);
      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new UnsupportedOperationException(
            "Hive internal error: conversion of " + inputOI.getTypeName() + " to " + outputOI.getTypeName()
                + "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object output = outputOI.create(0);
      int size = inputOI.getListLength(input);

      // Convert the elements
      outputOI.resize(output, size);
      for (int index = 0; index < size; index++) {
        Object inputElement = inputOI.getListElement(input, index);
        Object outputElement = elementConverter.convert(inputElement);
        outputOI.set(output, index, outputElement);
      }
      return output;
    }
  }

  /**
   * A converter class for Struct.
   */
  public class StructConverter implements Converter {

    StructObjectInspector inputOI;
    SettableStructObjectInspector outputOI;

    List<? extends StructField> inputFields;
    List<? extends StructField> outputFields;

    ArrayList<Converter> fieldConverters;

    public StructConverter(ObjectInspector inputOI, SettableStructObjectInspector outputOI) {
      if (inputOI instanceof StructObjectInspector) {
        this.inputOI = (StructObjectInspector) inputOI;
        this.outputOI = outputOI;
        inputFields = this.inputOI.getAllStructFieldRefs();
        outputFields = outputOI.getAllStructFieldRefs();

        // If the output has some extra fields, set them to NULL.
        int minFields = Math.min(inputFields.size(), outputFields.size());
        fieldConverters = new ArrayList<Converter>(minFields);
        for (int f = 0; f < minFields; f++) {
          fieldConverters.add(getConverter(inputFields.get(f).getFieldObjectInspector(),
              outputFields.get(f).getFieldObjectInspector()));
        }
      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new UnsupportedOperationException(
            "Hive internal error: conversion of " + inputOI.getTypeName() + " to " + outputOI.getTypeName()
                + "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object output = outputOI.create();
      int minFields = Math.min(inputFields.size(), outputFields.size());
      // Convert the fields
      for (int f = 0; f < minFields; f++) {
        Object inputFieldValue = inputOI.getStructFieldData(input, inputFields.get(f));
        Object outputFieldValue = fieldConverters.get(f).convert(inputFieldValue);
        outputOI.setStructFieldData(output, outputFields.get(f), outputFieldValue);
      }

      // set the extra fields to null
      for (int f = minFields; f < outputFields.size(); f++) {
        outputOI.setStructFieldData(output, outputFields.get(f), null);
      }

      return output;
    }
  }

  /**
   * A converter class for Map.
   */
  public class MapConverter implements Converter {

    MapObjectInspector inputOI;
    SettableMapObjectInspector outputOI;

    ObjectInspector inputKeyOI;
    ObjectInspector outputKeyOI;

    ObjectInspector inputValueOI;
    ObjectInspector outputValueOI;

    Converter keyConverter;
    Converter valueConverter;

    public MapConverter(ObjectInspector inputOI, SettableMapObjectInspector outputOI) {
      if (inputOI instanceof MapObjectInspector) {
        this.inputOI = (MapObjectInspector) inputOI;
        this.outputOI = outputOI;
        inputKeyOI = this.inputOI.getMapKeyObjectInspector();
        outputKeyOI = outputOI.getMapKeyObjectInspector();
        inputValueOI = this.inputOI.getMapValueObjectInspector();
        outputValueOI = outputOI.getMapValueObjectInspector();
        keyConverter = getConverter(inputKeyOI, outputKeyOI);
        valueConverter = getConverter(inputValueOI, outputValueOI);
      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new UnsupportedOperationException(
            "Hive internal error: conversion of " + inputOI.getTypeName() + " to " + outputOI.getTypeName()
                + "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      Object output = outputOI.create();

      // NOTE: This code tries to get all key-value pairs out of the map.
      // It's not very efficient. The more efficient way should be to let MapOI
      // return an Iterator. This is currently not supported by MapOI yet.

      Map<?, ?> map = inputOI.getMap(input);

      // Convert the key/value pairs
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object inputKey = entry.getKey();
        Object inputValue = entry.getValue();
        Object outputKey = keyConverter.convert(inputKey);
        Object outputValue = valueConverter.convert(inputValue);
        outputOI.put(output, outputKey, outputValue);
      }
      return output;
    }
  }
}
