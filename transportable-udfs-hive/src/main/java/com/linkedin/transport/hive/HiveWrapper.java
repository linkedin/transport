/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.hive;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdData;
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
import com.linkedin.transport.hive.types.HiveArrayType;
import com.linkedin.transport.hive.types.HiveBooleanType;
import com.linkedin.transport.hive.types.HiveBinaryType;
import com.linkedin.transport.hive.types.HiveDoubleType;
import com.linkedin.transport.hive.types.HiveFloatType;
import com.linkedin.transport.hive.types.HiveIntegerType;
import com.linkedin.transport.hive.types.HiveLongType;
import com.linkedin.transport.hive.types.HiveMapType;
import com.linkedin.transport.hive.types.HiveStringType;
import com.linkedin.transport.hive.types.HiveStructType;
import com.linkedin.transport.hive.types.HiveUnknownType;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;


public final class HiveWrapper {

  private HiveWrapper() {
  }

  public static StdData createStdData(Object hiveData, ObjectInspector hiveObjectInspector, StdFactory stdFactory) {
    if (hiveObjectInspector instanceof IntObjectInspector) {
      return new HiveInteger(hiveData, (IntObjectInspector) hiveObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof LongObjectInspector) {
      return new HiveLong(hiveData, (LongObjectInspector) hiveObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof BooleanObjectInspector) {
      return new HiveBoolean(hiveData, (BooleanObjectInspector) hiveObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof StringObjectInspector) {
      return new HiveString(hiveData, (StringObjectInspector) hiveObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof FloatObjectInspector) {
      return new HiveFloat(hiveData, (FloatObjectInspector) hiveObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof DoubleObjectInspector) {
      return new HiveDouble(hiveData, (DoubleObjectInspector) hiveObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof BinaryObjectInspector) {
      return new HiveBinary(hiveData, (BinaryObjectInspector) hiveObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof ListObjectInspector) {
      ListObjectInspector listObjectInspector = (ListObjectInspector) hiveObjectInspector;
      return new HiveArray(hiveData, listObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof MapObjectInspector) {
      return new HiveMap(hiveData, hiveObjectInspector, stdFactory);
    } else if (hiveObjectInspector instanceof StructObjectInspector) {
      return new HiveStruct(((StructObjectInspector) hiveObjectInspector).getStructFieldsDataAsList(hiveData).toArray(),
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
      return new HiveStructType((StructObjectInspector) hiveObjectInspector);
    } else if (hiveObjectInspector instanceof VoidObjectInspector) {
      return new HiveUnknownType((VoidObjectInspector) hiveObjectInspector);
    }
    assert false : "Unrecognized Hive ObjectInspector: " + hiveObjectInspector.getClass();
    return null;
  }
}
