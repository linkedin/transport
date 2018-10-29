/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro.types;

import com.linkedin.transport.api.types.StdIntegerType;
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
