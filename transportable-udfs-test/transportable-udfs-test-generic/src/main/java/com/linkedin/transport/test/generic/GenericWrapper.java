/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic;

import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.test.generic.data.GenericArray;
import com.linkedin.transport.test.generic.data.GenericBoolean;
import com.linkedin.transport.test.generic.data.GenericInteger;
import com.linkedin.transport.test.generic.data.GenericLong;
import com.linkedin.transport.test.generic.data.GenericMap;
import com.linkedin.transport.test.generic.data.GenericString;
import com.linkedin.transport.test.generic.data.GenericStruct;
import com.linkedin.transport.test.spi.Row;
import com.linkedin.transport.test.spi.types.ArrayTestType;
import com.linkedin.transport.test.spi.types.BooleanTestType;
import com.linkedin.transport.test.spi.types.IntegerTestType;
import com.linkedin.transport.test.spi.types.LongTestType;
import com.linkedin.transport.test.spi.types.MapTestType;
import com.linkedin.transport.test.spi.types.StringTestType;
import com.linkedin.transport.test.spi.types.StructTestType;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.test.spi.types.UnknownTestType;
import java.util.List;
import java.util.Map;


public class GenericWrapper {

  private GenericWrapper() {
  }

  public static StdData createStdData(Object data, TestType dataType) {
    if (dataType instanceof UnknownTestType) {
      return null;
    } else if (dataType instanceof IntegerTestType) {
      return new GenericInteger((Integer) data);
    } else if (dataType instanceof LongTestType) {
      return new GenericLong((Long) data);
    } else if (dataType instanceof BooleanTestType) {
      return new GenericBoolean((Boolean) data);
    } else if (dataType instanceof StringTestType) {
      return new GenericString((String) data);
    } else if (dataType instanceof ArrayTestType) {
      return new GenericArray((List<Object>) data, dataType);
    } else if (dataType instanceof MapTestType) {
      return new GenericMap((Map<Object, Object>) data, dataType);
    } else if (dataType instanceof StructTestType) {
      return new GenericStruct((Row) data, dataType);
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + dataType.getClass());
    }
  }

  public static StdType createStdType(TestType dataType) {
    return () -> dataType;
  }
}
