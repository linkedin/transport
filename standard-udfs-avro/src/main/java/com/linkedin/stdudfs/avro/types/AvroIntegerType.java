package com.linkedin.stdudfs.avro.types;

import com.linkedin.stdudfs.api.types.StdIntegerType;
import org.apache.avro.Schema;


public class AvroIntegerType implements StdIntegerType {
  final private Schema _schema;

  public AvroIntegerType(Schema schema) {
    _schema = schema;
  }

  @Override
  public Object underlyingType() {
    return _schema;
  }
}
