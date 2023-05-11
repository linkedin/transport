/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.types;

import com.linkedin.transport.api.types.StdStructType;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.avro.AvroWrapper;
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
  public List<String> fieldNames() {
    return _schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
  }

  @Override
  public List<? extends StdType> fieldTypes() {
    return _schema.getFields().stream().map(f -> AvroWrapper.createStdType(f.schema())).collect(Collectors.toList());
  }
}
