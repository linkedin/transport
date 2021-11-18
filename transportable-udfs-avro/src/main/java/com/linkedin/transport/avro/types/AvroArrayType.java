/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.types;

import com.linkedin.transport.api.types.ArrayType;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.avro.AvroConverters;
import org.apache.avro.Schema;


public class AvroArrayType implements ArrayType {
  private final Schema _schema;

  public AvroArrayType(Schema schema) {
    _schema = schema;
  }

  @Override
  public DataType elementType() {
    return AvroConverters.toTransportType(_schema.getElementType());
  }

  @Override
  public Object underlyingType() {
    return _schema;
  }
}
