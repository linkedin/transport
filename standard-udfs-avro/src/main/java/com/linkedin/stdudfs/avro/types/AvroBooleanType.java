package com.linkedin.stdudfs.avro.types;

import com.linkedin.stdudfs.api.types.StdBooleanType;
import org.apache.avro.Schema;


public class AvroBooleanType implements StdBooleanType {
  final private Schema _schema;

  public AvroBooleanType(Schema schema) {
    _schema = schema;
  }

  @Override
  public Object underlyingType() {
    return _schema;
  }
}
