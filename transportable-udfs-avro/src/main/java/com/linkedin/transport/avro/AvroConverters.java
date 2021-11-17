/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.avro.data.AvroArrayData;
import com.linkedin.transport.avro.data.AvroMapData;
import com.linkedin.transport.avro.data.AvroRowData;
import com.linkedin.transport.avro.types.AvroArrayType;
import com.linkedin.transport.avro.types.AvroBinaryType;
import com.linkedin.transport.avro.types.AvroBooleanType;
import com.linkedin.transport.avro.types.AvroDoubleType;
import com.linkedin.transport.avro.types.AvroFloatType;
import com.linkedin.transport.avro.types.AvroIntegerType;
import com.linkedin.transport.avro.types.AvroLongType;
import com.linkedin.transport.avro.types.AvroMapType;
import com.linkedin.transport.avro.types.AvroStringType;
import com.linkedin.transport.avro.types.AvroRowType;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


public class AvroConverters {

  private AvroConverters() {
  }

  public static Object toTransportData(Object avroData, Schema avroSchema) {
    switch (avroSchema.getType()) {
      case INT:
      case LONG:
      case BOOLEAN:
      case FLOAT:
      case DOUBLE:
      case BYTES:
        return avroData;
      case STRING:
      case ENUM:
        if (avroData == null) {
          return null;
        } else {
          return avroData.toString();
        }
      case ARRAY:
        return new AvroArrayData((GenericArray<Object>) avroData, avroSchema);
      case MAP:
        return new AvroMapData((Map<Object, Object>) avroData, avroSchema);
      case RECORD:
        return new AvroRowData((GenericRecord) avroData, avroSchema);
      case UNION: {
        Schema nonNullableType = getNonNullComponent(avroSchema);
        if (avroData == null) {
          return null;
        }
        return toTransportData(avroData, nonNullableType);
      }
      case NULL:
        return null;
      default:
        throw new RuntimeException("Unrecognized Avro Schema: " + avroSchema.getClass());
    }
  }

  public static Object toPlatformData(Object transportData) {
    if (transportData instanceof Integer || transportData instanceof Long || transportData instanceof Double
        || transportData instanceof Boolean || transportData instanceof ByteBuffer) {
      return transportData;
    } else if (transportData instanceof String) {
      return transportData == null ? null : new Utf8((String) transportData);
    } else {
      return transportData == null ? null : ((PlatformData) transportData).getUnderlyingData();
    }
  }


  /**
   * Returns a non null component of a simple union schema. The supported union schema must have
   * only two fields where one of them is null type, the other is returned.
   */
  private static Schema getNonNullComponent(Schema unionSchema) {
    List<Schema> types = unionSchema.getTypes();
    if (types.size() == 2) {
      if (types.get(0).getType().equals(Schema.Type.NULL)) {
        return types.get(1);
      }

      if (types.get(1).getType().equals(Schema.Type.NULL)) {
        return types.get(0);
      }
    }
    throw new RuntimeException("Unsupported union type: " + unionSchema);
  }

  public static DataType toTransportType(Schema avroSchema) {
    switch (avroSchema.getType()) {
      case INT:
        return new AvroIntegerType(avroSchema);
      case LONG:
        return new AvroLongType(avroSchema);
      case BOOLEAN:
        return new AvroBooleanType(avroSchema);
      case STRING:
        return new AvroStringType(avroSchema);
      case FLOAT:
        return new AvroFloatType(avroSchema);
      case DOUBLE:
        return new AvroDoubleType(avroSchema);
      case BYTES:
        return new AvroBinaryType(avroSchema);
      case ARRAY:
        return new AvroArrayType(avroSchema);
      case MAP:
        return new AvroMapType(avroSchema);
      case RECORD:
        return new AvroRowType(avroSchema);
      case UNION: {
        Schema nonNullableType = getNonNullComponent(avroSchema);
        return toTransportType(nonNullableType);
      }
      default:
        throw new RuntimeException("Unrecognized Avro Schema: " + avroSchema.getClass());
    }
  }
}
