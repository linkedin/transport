/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.test.generic.data.GenericArrayData;
import com.linkedin.transport.test.generic.data.GenericMapData;
import com.linkedin.transport.test.generic.data.GenericStruct;
import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.types.ArrayTestType;
import com.linkedin.transport.test.spi.types.BooleanTestType;
import com.linkedin.transport.test.spi.types.BinaryTestType;
import com.linkedin.transport.test.spi.types.DoubleTestType;
import com.linkedin.transport.test.spi.types.FloatTestType;
import com.linkedin.transport.test.spi.types.IntegerTestType;
import com.linkedin.transport.test.spi.types.LongTestType;
import com.linkedin.transport.test.spi.types.MapTestType;
import com.linkedin.transport.test.spi.types.StringTestType;
import com.linkedin.transport.test.spi.types.StructTestType;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.test.spi.types.UnknownTestType;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;


public class GenericWrapper {

  private GenericWrapper() {
  }

  public static Object createStdData(Object data, TestType dataType) {
    if (dataType instanceof UnknownTestType) {
      return null;
    } else if (dataType instanceof IntegerTestType || dataType instanceof LongTestType
        || dataType instanceof FloatTestType || dataType instanceof DoubleTestType
        || dataType instanceof BooleanTestType || dataType instanceof StringTestType || dataType instanceof BinaryTestType) {
      return data;
    } else if (dataType instanceof ArrayTestType) {
      return new GenericArrayData((List<Object>) data, dataType);
    } else if (dataType instanceof MapTestType) {
      return new GenericMapData((Map<Object, Object>) data, dataType);
    } else if (dataType instanceof StructTestType) {
      return new GenericStruct((Row) data, dataType);
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + dataType.getClass());
    }
  }

  public static Object getPlatformData(Object transportData) {
    if (transportData == null) {
      return null;
    } else {
      if (transportData instanceof Integer || transportData instanceof Long || transportData instanceof Float
          || transportData instanceof Double || transportData instanceof Boolean || transportData instanceof ByteBuffer
          || transportData instanceof String) {
        return transportData;
      } else {
        return ((PlatformData) transportData).getUnderlyingData();
      }
    }
  }

  public static DataType createStdType(TestType dataType) {
    return () -> dataType;
  }
}
