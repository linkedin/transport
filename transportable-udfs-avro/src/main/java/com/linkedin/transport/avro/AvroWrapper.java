/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.avro.data.AvroArrayData;
import com.linkedin.transport.avro.data.AvroMapData;
import com.linkedin.transport.avro.data.AvroRowData;
import com.linkedin.transport.avro.types.AvroArrayType;
import com.linkedin.transport.avro.types.AvroBooleanType;
import com.linkedin.transport.avro.types.AvroIntegerType;
import com.linkedin.transport.avro.types.AvroLongType;
import com.linkedin.transport.avro.types.AvroMapType;
import com.linkedin.transport.avro.types.AvroStringType;
import com.linkedin.transport.avro.types.AvroRowType;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


public class AvroWrapper {

  private AvroWrapper() {
  }

  public static Object createStdData(Object avroData, Schema avroSchema) {
    switch (avroSchema.getType()) {
      case INT:
      case LONG:
      case BOOLEAN:
        return avroData;
      case STRING:
        return avroData == null? null : avroData.toString();
      case ARRAY:
        return new AvroArrayData((GenericArray<Object>) avroData, avroSchema);
      case MAP:
        return new AvroMapData((Map<Object, Object>) avroData, avroSchema);
      case RECORD:
        return new AvroRowData((GenericRecord) avroData, avroSchema);
      case NULL:
        return null;
      default:
        throw new RuntimeException("Unrecognized Avro Schema: " + avroSchema.getClass());
    }
  }

  public static Object getPlatformData(Object transportData) {
    if (transportData instanceof Integer || transportData instanceof Long || transportData instanceof Boolean) {
      return transportData;
    } else if (transportData instanceof String) {
      return transportData == null? null : new Utf8((String) transportData);
    } else {
      return transportData == null ? null : ((PlatformData) transportData).getUnderlyingData();
    }
  }


  public static StdType createStdType(Schema avroSchema) {
    switch (avroSchema.getType()) {
      case INT:
        return new AvroIntegerType(avroSchema);
      case LONG:
        return new AvroLongType(avroSchema);
      case BOOLEAN:
        return new AvroBooleanType(avroSchema);
      case STRING:
        return new AvroStringType(avroSchema);
      case ARRAY:
        return new AvroArrayType(avroSchema);
      case MAP:
        return new AvroMapType(avroSchema);
      case RECORD:
        return new AvroRowType(avroSchema);
      default:
        throw new RuntimeException("Unrecognized Avro Schema: " + avroSchema.getClass());
    }
  }
}
