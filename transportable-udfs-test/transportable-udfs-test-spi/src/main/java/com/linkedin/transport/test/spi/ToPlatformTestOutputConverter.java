/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi;

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


/**
 * Converts output data defined using Java objects in the {@link TestCase} to the format used by the platform for
 * expressing expected test output when using its native testing framework
 */
public interface ToPlatformTestOutputConverter {

  default Object convertToTestOutput(Object data, TestType dataType) {
    if (data == null || dataType instanceof UnknownTestType) {
      return getNullData();
    } else if (dataType instanceof IntegerTestType) {
      return getIntegerData((Integer) data);
    } else if (dataType instanceof LongTestType) {
      return getLongData((Long) data);
    } else if (dataType instanceof BooleanTestType) {
      return getBooleanData((Boolean) data);
    } else if (dataType instanceof StringTestType) {
      return getStringData((String) data);
    } else if (dataType instanceof ArrayTestType) {
      return getArrayData((List<Object>) data, ((ArrayTestType) dataType).getElementType());
    } else if (dataType instanceof MapTestType) {
      return getMapData((Map<Object, Object>) data, ((MapTestType) dataType).getKeyType(),
          ((MapTestType) dataType).getValueType());
    } else if (dataType instanceof StructTestType) {
      return getStructData((Row) data, ((StructTestType) dataType).getFieldTypes(),
          ((StructTestType) dataType).getFieldNames());
    } else {
      throw new UnsupportedOperationException("Unsupported data type: " + dataType.getClass());
    }
  }

  default Object getNullData() {
    return null;
  }

  default Object getIntegerData(Integer value) {
    return value;
  }

  default Object getLongData(Long value) {
    return value;
  }

  default Object getBooleanData(Boolean value) {
    return value;
  }

  default Object getStringData(String value) {
    return value;
  }

  Object getArrayData(List<Object> array, TestType elementType);

  Object getMapData(Map<Object, Object> map, TestType mapKeyType, TestType mapValueType);

  Object getStructData(Row struct, List<TestType> fieldTypes, List<String> fieldNames);
}
