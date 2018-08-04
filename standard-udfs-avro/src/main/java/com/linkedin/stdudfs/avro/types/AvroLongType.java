package com.linkedin.stdudfs.avro.types;

import com.linkedin.stdudfs.api.types.StdLongType;
import org.apache.avro.Schema;


public class AvroLongType implements StdLongType {
  final private Schema _schema;

  public AvroLongType(Schema schema) {
    _schema = schema;
  }

  @Override
  public Object underlyingType() {
    return _schema;
  }
}
