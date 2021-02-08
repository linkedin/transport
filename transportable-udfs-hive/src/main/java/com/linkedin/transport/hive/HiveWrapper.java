/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.types.StdType;
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


public final class HiveWrapper {

  private HiveWrapper() {
  }

  public static Object createStdData(Object hiveData, ObjectInspector hiveObjectInspector, StdFactory stdFactory) {
    if (hiveObjectInspector instanceof IntObjectInspector || hiveObjectInspector instanceof LongObjectInspector
        || hiveObjectInspector instanceof FloatObjectInspector || hiveObjectInspector instanceof DoubleObjectInspector
        || hiveObjectInspector instanceof BooleanObjectInspector
        || hiveObjectInspector instanceof StringObjectInspector) {
      return ((PrimitiveObjectInspector) hiveObjectInspector).getPrimitiveJavaObject(hiveData);
    } else if (hiveObjectInspector instanceof BinaryObjectInspector) {
      BinaryObjectInspector binaryObjectInspector = (BinaryObjectInspector) hiveObjectInspector;
      return hiveData == null ? null : ByteBuffer.wrap(binaryObjectInspector.getPrimitiveJavaObject(hiveData));
    } else if (hiveObjectInspector instanceof ListObjectInspector) {
      ListObjectInspector listObjectInspector = (ListObjectInspector) hiveObjectInspector;
      return new HiveArrayData(hiveData, listObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof MapObjectInspector) {
      return new HiveMapData(hiveData, hiveObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof StructObjectInspector) {
      return new HiveRowData(((StructObjectInspector) hiveObjectInspector).getStructFieldsDataAsList(hiveData).toArray(),
          hiveObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof VoidObjectInspector) {
      return null;
    }
    assert false : "Unrecognized Hive ObjectInspector: " + hiveObjectInspector.getClass();
    return null;
  }

  public static StdType createStdType(ObjectInspector hiveObjectInspector) {
    if (hiveObjectInspector instanceof IntObjectInspector) {
      return new HiveIntegerType((IntObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof LongObjectInspector) {
      return new HiveLongType((LongObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof BooleanObjectInspector) {
      return new HiveBooleanType((BooleanObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof StringObjectInspector) {
      return new HiveStringType((StringObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof FloatObjectInspector) {
      return new HiveFloatType((FloatObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof DoubleObjectInspector) {
      return new HiveDoubleType((DoubleObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof BinaryObjectInspector) {
      return new HiveBinaryType((BinaryObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof ListObjectInspector) {
      return new HiveArrayType((ListObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof MapObjectInspector) {
      return new HiveMapType((MapObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof StructObjectInspector) {
      return new HiveRowType((StructObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof VoidObjectInspector) {
      return new HiveUnknownType((VoidObjectInspector) hiveObjectInspector);
    }
    assert false : "Unrecognized Hive ObjectInspector: " + hiveObjectInspector.getClass();
    return null;
  }

  public static Object getPlatformDataForObjectInspector(Object transportData, ObjectInspector oi) {
    if (transportData == null) {
      return null;
    } else if (oi instanceof IntObjectInspector) {
      return ((SettableIntObjectInspector) oi).create((Integer) transportData);
    } else if (oi instanceof LongObjectInspector) {
      return ((SettableLongObjectInspector) oi).create((Long) transportData);
    } else if (oi instanceof FloatObjectInspector) {
      return ((SettableFloatObjectInspector) oi).create((Float) transportData);
    } else if (oi instanceof DoubleObjectInspector) {
      return ((SettableDoubleObjectInspector) oi).create((Double) transportData);
    } else if (oi instanceof BooleanObjectInspector) {
      return ((SettableBooleanObjectInspector) oi).create((Boolean) transportData);
    } else if (oi instanceof StringObjectInspector) {
      return ((SettableStringObjectInspector) oi).create((String) transportData);
    } else if (oi instanceof BinaryObjectInspector) {
      return ((SettableBinaryObjectInspector) oi).create(((ByteBuffer) transportData).array());
    } else {
      return ((HiveData) transportData).getUnderlyingDataForObjectInspector(oi);
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
