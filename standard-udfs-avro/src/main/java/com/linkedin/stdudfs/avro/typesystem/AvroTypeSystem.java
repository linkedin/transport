/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
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

