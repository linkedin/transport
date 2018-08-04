package com.linkedin.stdudfs.avro.common;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.*;


public class SchemaFactory {
  private SchemaFactory() {
  }

  final public static Schema INTEGER = Schema.create(Type.INT);
  final public static Schema LONG = Schema.create(Type.LONG);
  final public static Schema BOOLEAN = Schema.create(Type.BOOLEAN);
  final public static Schema STRING = Schema.create(Type.STRING);
  final public static Schema NULL = Schema.create(Type.NULL);

  public static Schema array(Schema elementSchema) {
    return Schema.createArray(elementSchema);
  }

  public static Schema map(Schema keySchema, Schema valueSchema) {
    if (keySchema.getType() != Type.STRING) {
      throw new RuntimeException(
          "Avro map keys can be of STRING type only. Received map key of type: " + keySchema.getType().getName());
    }
    return Schema.createMap(valueSchema);
  }

  public static Schema struct(Schema... fieldSchemas) {
    return Schema.createRecord(IntStream.range(0, fieldSchemas.length)
        .mapToObj(i -> new Schema.Field("field" + i, fieldSchemas[i], null, null))
        .collect(Collectors.toList()));
  }
}
