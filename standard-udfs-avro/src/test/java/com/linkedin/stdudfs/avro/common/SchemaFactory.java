/**
 * BSD 2-CLAUSE LICENSE
 *
 * Copyright 2018 LinkedIn Corporation.
 * All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.linkedin.stdudfs.avro.common;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.*;


public class SchemaFactory {
  private SchemaFactory() {
  }

  final public static Schema INTEGER = Schema.create(Type.INT);
  final public static Schema LONG = Schema.create(Type.LONG);
  final public static Schema BOOLEAN = Schema.create(Type.BOOLEAN);
  final public static Schema STRING = Schema.create(Type.STRING);
  final public static Schema NULL = Schema.create(Type.NULL);

  public static Schema array(Schema elementSchema) {
    return Schema.createArray(elementSchema);
  }

  public static Schema map(Schema keySchema, Schema valueSchema) {
    if (keySchema.getType() != Type.STRING) {
      throw new RuntimeException(
          "Avro map keys can be of STRING type only. Received map key of type: " + keySchema.getType().getName());
    }
    return Schema.createMap(valueSchema);
  }

  public static Schema struct(Schema... fieldSchemas) {
    return Schema.createRecord(IntStream.range(0, fieldSchemas.length)
        .mapToObj(i -> new Schema.Field("field" + i, fieldSchemas[i], null, null))
        .collect(Collectors.toList()));
  }
}
