/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive;

import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.hive.data.HiveArrayData;
import com.linkedin.transport.hive.data.HiveData;
import com.linkedin.transport.hive.data.HiveMapData;
import com.linkedin.transport.hive.data.HiveRowData;
import com.linkedin.transport.hive.types.HiveArrayType;
import com.linkedin.transport.hive.types.HiveBooleanType;
import com.linkedin.transport.hive.types.HiveBinaryType;
import com.linkedin.transport.hive.types.HiveDoubleType;
import com.linkedin.transport.hive.types.HiveFloatType;
import com.linkedin.transport.hive.types.HiveIntegerType;
import com.linkedin.transport.hive.types.HiveLongType;
import com.linkedin.transport.hive.types.HiveMapType;
import com.linkedin.transport.hive.types.HiveStringType;
import com.linkedin.transport.hive.types.HiveRowType;
import com.linkedin.transport.hive.types.HiveUnknownType;
import java.nio.ByteBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;


public final class HiveConverters {

  private HiveConverters() {
  }

  public static Object toTransportData(Object data, ObjectInspector objectInspector, TypeFactory typeFactory) {
    if (objectInspector instanceof IntObjectInspector || objectInspector instanceof LongObjectInspector
        || objectInspector instanceof FloatObjectInspector || objectInspector instanceof DoubleObjectInspector
        || objectInspector instanceof BooleanObjectInspector
        || objectInspector instanceof StringObjectInspector) {
      return ((PrimitiveObjectInspector) objectInspector).getPrimitiveJavaObject(data);
    } else if (objectInspector instanceof BinaryObjectInspector) {
      BinaryObjectInspector binaryObjectInspector = (BinaryObjectInspector) objectInspector;
      return data == null ? null : ByteBuffer.wrap(binaryObjectInspector.getPrimitiveJavaObject(data));
    } else if (objectInspector instanceof ListObjectInspector) {
      ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
      return new HiveArrayData(data, listObjectInspector, typeFactory);
    } else if (objectInspector instanceof MapObjectInspector) {
      return new HiveMapData(data, objectInspector, typeFactory);
    } else if (objectInspector instanceof StructObjectInspector) {
      return new HiveRowData(((StructObjectInspector) objectInspector).getStructFieldsDataAsList(data).toArray(),
          objectInspector, typeFactory);
    } else if (objectInspector instanceof VoidObjectInspector) {
      return null;
    }
    assert false : "Unrecognized Hive ObjectInspector: " + objectInspector.getClass();
    return null;
  }

  public static DataType toTransportType(ObjectInspector objectInspector) {
    if (objectInspector instanceof IntObjectInspector) {
      return new HiveIntegerType((IntObjectInspector) objectInspector);
    } else if (objectInspector instanceof LongObjectInspector) {
      return new HiveLongType((LongObjectInspector) objectInspector);
    } else if (objectInspector instanceof BooleanObjectInspector) {
      return new HiveBooleanType((BooleanObjectInspector) objectInspector);
    } else if (objectInspector instanceof StringObjectInspector) {
      return new HiveStringType((StringObjectInspector) objectInspector);
    } else if (objectInspector instanceof FloatObjectInspector) {
      return new HiveFloatType((FloatObjectInspector) objectInspector);
    } else if (objectInspector instanceof DoubleObjectInspector) {
      return new HiveDoubleType((DoubleObjectInspector) objectInspector);
    } else if (objectInspector instanceof BinaryObjectInspector) {
      return new HiveBinaryType((BinaryObjectInspector) objectInspector);
    } else if (objectInspector instanceof ListObjectInspector) {
      return new HiveArrayType((ListObjectInspector) objectInspector);
    } else if (objectInspector instanceof MapObjectInspector) {
      return new HiveMapType((MapObjectInspector) objectInspector);
    } else if (objectInspector instanceof StructObjectInspector) {
      return new HiveRowType((StructObjectInspector) objectInspector);
    } else if (objectInspector instanceof VoidObjectInspector) {
      return new HiveUnknownType((VoidObjectInspector) objectInspector);
    }
    assert false : "Unrecognized Hive ObjectInspector: " + objectInspector.getClass();
    return null;
  }

  public static Object toPlatformData(Object transportData, ObjectInspector objectInspector) {
    if (transportData == null) {
      return null;
    } else if (objectInspector instanceof IntObjectInspector) {
      return ((SettableIntObjectInspector) objectInspector).create((Integer) transportData);
    } else if (objectInspector instanceof LongObjectInspector) {
      return ((SettableLongObjectInspector) objectInspector).create((Long) transportData);
    } else if (objectInspector instanceof FloatObjectInspector) {
      return ((SettableFloatObjectInspector) objectInspector).create((Float) transportData);
    } else if (objectInspector instanceof DoubleObjectInspector) {
      return ((SettableDoubleObjectInspector) objectInspector).create((Double) transportData);
    } else if (objectInspector instanceof BooleanObjectInspector) {
      return ((SettableBooleanObjectInspector) objectInspector).create((Boolean) transportData);
    } else if (objectInspector instanceof StringObjectInspector) {
      return ((SettableStringObjectInspector) objectInspector).create((String) transportData);
    } else if (objectInspector instanceof BinaryObjectInspector) {
      return ((SettableBinaryObjectInspector) objectInspector).create(((ByteBuffer) transportData).array());
    } else {
      return ((HiveData) transportData).getUnderlyingDataForObjectInspector(objectInspector);
    }
  }

  public static Object getStandardObject(Object transportData) {
    if (transportData == null) {
      return null;
    } else if (transportData instanceof Integer) {
      return PrimitiveObjectInspectorFactory.writableIntObjectInspector.create((Integer) transportData);
    } else if (transportData instanceof Long) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector.create((Long) transportData);
    } else if (transportData instanceof Float) {
      return PrimitiveObjectInspectorFactory.writableFloatObjectInspector.create((Float) transportData);
    } else if (transportData instanceof Double) {
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector.create((Double) transportData);
    } else if (transportData instanceof Boolean) {
      return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector.create((Boolean) transportData);
    } else if (transportData instanceof String) {
      return PrimitiveObjectInspectorFactory.writableStringObjectInspector.create((String) transportData);
    } else if (transportData instanceof ByteBuffer) {
      return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector.create(((ByteBuffer) transportData).array());
    } else {
      return ((HiveData) transportData).getUnderlyingDataForObjectInspector(
          ((HiveData) transportData).getUnderlyingObjectInspector()
      );
    }
  }
}
