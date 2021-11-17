/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.types;

import com.linkedin.transport.api.types.MapType;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.avro.AvroWrapper;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.Type.*;


public class AvroMapType implements MapType {
  private final Schema _schema;

  public AvroMapType(Schema schema) {
    _schema = schema;
  }

  @Override
  public DataType keyType() {
    return AvroWrapper.createStdType(Schema.create(STRING));
  }

  @Override
  public DataType valueType() {
    return AvroWrapper.createStdType(_schema.getValueType());
  }

  @Override
  public Object underlyingType() {
    return _schema;
  }
}
