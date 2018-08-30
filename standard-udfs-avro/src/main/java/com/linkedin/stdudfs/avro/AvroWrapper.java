/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.linkedin.stdudfs.avro;

import com.linkedin.stdudfs.api.data.StdData;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.avro.data.AvroArray;
import com.linkedin.stdudfs.avro.data.AvroBoolean;
import com.linkedin.stdudfs.avro.data.AvroInteger;
import com.linkedin.stdudfs.avro.data.AvroLong;
import com.linkedin.stdudfs.avro.data.AvroMap;
import com.linkedin.stdudfs.avro.data.AvroString;
import com.linkedin.stdudfs.avro.data.AvroStruct;
import com.linkedin.stdudfs.avro.types.AvroArrayType;
import com.linkedin.stdudfs.avro.types.AvroBooleanType;
import com.linkedin.stdudfs.avro.types.AvroIntegerType;
import com.linkedin.stdudfs.avro.types.AvroLongType;
import com.linkedin.stdudfs.avro.types.AvroMapType;
import com.linkedin.stdudfs.avro.types.AvroStringType;
import com.linkedin.stdudfs.avro.types.AvroStructType;
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
