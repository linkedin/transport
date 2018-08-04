package com.linkedin.stdudfs.avro.types;

import com.linkedin.stdudfs.api.types.StdStringType;
import org.apache.avro.Schema;


public class AvroStringType implements StdStringType {
  final private Schema _schema;

  public AvroStringType(Schema schema) {
    _schema = schema;
  }

  @Override
  public Object underlyingType() {
    return _schema;
  }
}
