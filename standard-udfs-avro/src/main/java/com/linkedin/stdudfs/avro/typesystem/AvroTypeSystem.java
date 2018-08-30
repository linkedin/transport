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
package com.linkedin.stdudfs.avro.typesystem;

import com.linkedin.stdudfs.typesystem.AbstractTypeSystem;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.Type.*;


public class AvroTypeSystem extends AbstractTypeSystem<Schema> {
  @Override
  protected Schema getArrayElementType(Schema dataType) {
    return dataType.getElementType();
  }

  @Override
  protected Schema getMapKeyType(Schema dataType) {
    return Schema.create(STRING);
  }

  @Override
  protected Schema getMapValueType(Schema dataType) {
    return dataType.getValueType();
  }

  @Override
  protected List<Schema> getStructFieldTypes(Schema dataType) {
    return dataType.getFields().stream().map(f -> f.schema()).collect(Collectors.toList());
  }

  @Override
  protected boolean isUnknownType(Schema dataType) {
    return dataType.getType() == NULL;
  }

  @Override
  protected boolean isBooleanType(Schema dataType) {
    return dataType.getType() == BOOLEAN;
  }

  @Override
  protected boolean isIntegerType(Schema dataType) {
    return dataType.getType() == INT;
  }

  @Override
  protected boolean isLongType(Schema dataType) {
    return dataType.getType() == LONG;
  }

  @Override
  protected boolean isStringType(Schema dataType) {
    return dataType.getType() == STRING;
  }

  @Override
  protected boolean isArrayType(Schema dataType) {
    return dataType.getType() == ARRAY;
  }

  @Override
  protected boolean isMapType(Schema dataType) {
    return dataType.getType() == MAP;
  }

  @Override
  protected boolean isStructType(Schema dataType) {
    return dataType.getType() == RECORD;
  }

  @Override
  protected Schema createBooleanType() {
    return Schema.create(BOOLEAN);
  }

  @Override
  protected Schema createIntegerType() {
    return Schema.create(INT);
  }

  @Override
  protected Schema createLongType() {
    return Schema.create(LONG);
  }

  @Override
  protected Schema createStringType() {
    return Schema.create(STRING);
  }

  @Override
  protected Schema createUnknownType() {
    return Schema.create(NULL);
  }

  @Override
  protected Schema createArrayType(Schema elementType) {
    return Schema.createArray(elementType);
  }

  @Override
  protected Schema createMapType(Schema keyType, Schema valueType) {
    if (keyType.getType() != STRING) {
      throw new RuntimeException(
          "Avro map keys can be of STRING type only. Received map key of type: " + keyType.getType().getName());
    }
    return Schema.createMap(valueType);
  }

  @Override
  protected Schema createStructType(List<String> fieldNames, List<Schema> fieldTypes) {
    return Schema.createRecord(IntStream.range(0, fieldTypes.size())
        .mapToObj(i -> new Schema.Field(
            fieldNames == null ? "field" + i : fieldNames.get(i),
            fieldTypes.get(i), null, null
        ))
        .collect(Collectors.toList()));
  }
}

