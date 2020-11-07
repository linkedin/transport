/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.avro.data.AvroArray;
import com.linkedin.transport.avro.data.AvroBinary;
import com.linkedin.transport.avro.data.AvroBoolean;
import com.linkedin.transport.avro.data.AvroDouble;
import com.linkedin.transport.avro.data.AvroFloat;
import com.linkedin.transport.avro.data.AvroInteger;
import com.linkedin.transport.avro.data.AvroLong;
import com.linkedin.transport.avro.data.AvroMap;
import com.linkedin.transport.avro.data.AvroString;
import com.linkedin.transport.avro.data.AvroStruct;
import com.linkedin.transport.avro.types.AvroArrayType;
import com.linkedin.transport.avro.types.AvroBinaryType;
import com.linkedin.transport.avro.types.AvroBooleanType;
import com.linkedin.transport.avro.types.AvroDoubleType;
import com.linkedin.transport.avro.types.AvroFloatType;
import com.linkedin.transport.avro.types.AvroIntegerType;
import com.linkedin.transport.avro.types.AvroLongType;
import com.linkedin.transport.avro.types.AvroMapType;
import com.linkedin.transport.avro.types.AvroStringType;
import com.linkedin.transport.avro.types.AvroStructType;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;


public class AvroWrapper {

  private AvroWrapper() {
  }

  public static StdData createStdData(Object avroData, Schema avroSchema) {
    switch (avroSchema.getType()) {
      case INT: {
        if (!(avroData instanceof Integer)) {
          throw new IllegalArgumentException("Unsupported type for Avro integer: " + avroData.getClass());
        }
        return new AvroInteger((Integer) avroData);
      }
      case LONG: {
        if (!(avroData instanceof Long)) {
          throw new IllegalArgumentException("Unsupported type for Avro long: " + avroData.getClass());
        }
        return new AvroLong((Long) avroData);
      }
      case BOOLEAN: {
        if (!(avroData instanceof Boolean)) {
          throw new IllegalArgumentException("Unsupported type for Avro boolean: " + avroData.getClass());
        }
        return new AvroBoolean((Boolean) avroData);
      }
      case STRING: {
        if (avroData instanceof Utf8) {
          return new AvroString((Utf8) avroData);
        } else if (avroData instanceof String) {
          return new AvroString(new Utf8((String) avroData));
        }
        throw new IllegalArgumentException("Unsupported type for Avro string: " + avroData.getClass());
      }
      case FLOAT: {
        if (!(avroData instanceof Float)) {
          throw new IllegalArgumentException("Unsupported type for Avro float: " + avroData.getClass());
        }
        return new AvroFloat((Float) avroData);
      }
      case DOUBLE: {
        if (!(avroData instanceof Double)) {
          throw new IllegalArgumentException("Unsupported type for Avro double: " + avroData.getClass());
        }
        return new AvroDouble((Double) avroData);
      }
      case BYTES: {
        if (!(avroData instanceof ByteBuffer)) {
          throw new IllegalArgumentException("Unsupported type for Avro bytes: " + avroData.getClass());
        }
        return new AvroBinary((ByteBuffer) avroData);
      }
      case ARRAY: {
        if (!(avroData instanceof GenericArray)) {
          throw new IllegalArgumentException("Unsupported type for Avro array: " + avroData.getClass());
        }
        return new AvroArray((GenericArray<Object>) avroData, avroSchema);
      }
      case MAP: {
        if (!(avroData instanceof Map)) {
          throw new IllegalArgumentException("Unsupported type for Avro map: " + avroData.getClass());
        }
        return new AvroMap((Map<Object, Object>) avroData, avroSchema);
      }
      case RECORD: {
        if (!(avroData instanceof GenericRecord)) {
          throw new IllegalArgumentException("Unsupported type for Avro record: " + avroData.getClass());
        }
        return new AvroStruct((GenericRecord) avroData, avroSchema);
      }
      case UNION: {
        Schema nonNullableType = getNonNullComponent(avroSchema);
        if (avroData == null) {
          return null;
        }
        return createStdData(avroData, nonNullableType);
      }
      case NULL:
        return null;
      default:
        throw new RuntimeException("Unrecognized Avro Schema: " + avroSchema.getClass());
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
        return new AvroStructType(avroSchema);
      case UNION: {
        Schema nonNullableType = getNonNullComponent(avroSchema);
        return createStdType(nonNullableType);
      }
      default:
        throw new RuntimeException("Unrecognized Avro Schema: " + avroSchema.getClass());
    }
  }
}
