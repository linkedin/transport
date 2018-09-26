/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.avro.types;

import com.linkedin.stdudfs.api.types.StdMapType;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.avro.AvroWrapper;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.Type.*;


public class AvroMapType implements StdMapType {
  private final Schema _schema;

  public AvroMapType(Schema schema) {
    _schema = schema;
  }

  @Override
  public StdType keyType() {
    return AvroWrapper.createStdType(Schema.create(STRING));
  }

  @Override
  public StdType valueType() {
    return AvroWrapper.createStdType(_schema.getValueType());
  }

  @Override
  public Object underlyingType() {
    return _schema;
  }
}
