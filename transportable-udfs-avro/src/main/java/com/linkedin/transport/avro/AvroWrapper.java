/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

import com.linkedin.transport.api.data.StdData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.avro.data.AvroArray;
import com.linkedin.transport.avro.data.AvroBoolean;
import com.linkedin.transport.avro.data.AvroInteger;
import com.linkedin.transport.avro.data.AvroLong;
import com.linkedin.transport.avro.data.AvroMap;
import com.linkedin.transport.avro.data.AvroString;
import com.linkedin.transport.avro.data.AvroStruct;
import com.linkedin.transport.avro.types.AvroArrayType;
import com.linkedin.transport.avro.types.AvroBooleanType;
import com.linkedin.transport.avro.types.AvroIntegerType;
import com.linkedin.transport.avro.types.AvroLongType;
import com.linkedin.transport.avro.types.AvroMapType;
import com.linkedin.transport.avro.types.AvroStringType;
import com.linkedin.transport.avro.types.AvroStructType;
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
      case INT:
        return new AvroInteger((Integer) avroData);
      case LONG:
        return new AvroLong((Long) avroData);
      case BOOLEAN:
        return new AvroBoolean((Boolean) avroData);
      case STRING:
        return new AvroString((Utf8) avroData);
      case ARRAY:
        return new AvroArray((GenericArray<Object>) avroData, avroSchema);
      case MAP:
        return new AvroMap((Map<Object, Object>) avroData, avroSchema);
      case RECORD:
        return new AvroStruct((GenericRecord) avroData, avroSchema);
      case NULL:
        return null;
      default:
        throw new RuntimeException("Unrecognized Avro Schema: " + avroSchema.getClass());
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
        return new AvroStructType(avroSchema);
      default:
        throw new RuntimeException("Unrecognized Avro Schema: " + avroSchema.getClass());
    }
  }
}
