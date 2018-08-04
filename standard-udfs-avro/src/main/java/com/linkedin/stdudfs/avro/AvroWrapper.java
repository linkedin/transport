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
