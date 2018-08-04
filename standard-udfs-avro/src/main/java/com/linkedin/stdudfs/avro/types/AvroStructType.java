package com.linkedin.stdudfs.avro.types;

import com.linkedin.stdudfs.api.types.StdStructType;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.avro.AvroWrapper;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;


public class AvroStructType implements StdStructType {
  final private Schema _schema;

  public AvroStructType(Schema schema) {
    _schema = schema;
  }

  @Override
  public Object underlyingType() {
    return _schema;
  }

  @Override
  public List<? extends StdType> fieldTypes() {
    return _schema.getFields().stream().map(f -> AvroWrapper.createStdType(f.schema())).collect(Collectors.toList());
  }
}
