/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

import com.linkedin.transport.api.TypeFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.api.types.DataType;
import com.linkedin.transport.avro.data.AvroArrayData;
import com.linkedin.transport.avro.data.AvroMapData;
import com.linkedin.transport.avro.data.AvroRowData;
import com.linkedin.transport.avro.typesystem.AvroTypeFactory;
import com.linkedin.transport.typesystem.AbstractBoundVariables;
import com.linkedin.transport.typesystem.TypeSignature;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;

import static org.apache.avro.Schema.*;


public class AvroFactory implements TypeFactory {

  final AbstractBoundVariables<Schema> _boundVariables;
  final AvroTypeFactory _typeFactory;

  public AvroFactory(AbstractBoundVariables<Schema> boundVariables) {
    _boundVariables = boundVariables;
    _typeFactory = new AvroTypeFactory();
  }

  @Override
  public ArrayData createArray(DataType dataType, int size) {
    return new AvroArrayData((Schema) dataType.underlyingType(), size);
  }

  @Override
  public ArrayData createArray(DataType dataType) {
    return createArray(dataType, 0);
  }

  @Override
  public MapData createMap(DataType dataType) {
    return new AvroMapData((Schema) dataType.underlyingType());
  }

  @Override
  public RowData createStruct(List<String> fieldNames, List<DataType> fieldTypes) {
    if (fieldNames.size() != fieldTypes.size()) {
      throw new RuntimeException(
          "Field names and types are of different lengths: " + "Field names length is " + fieldNames.size() + ". "
              + "Field types length is: " + fieldTypes.size());
    }
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < fieldTypes.size(); i++) {
      fields.add(new Field(fieldNames.get(i), (Schema) fieldTypes.get(i).underlyingType(), null, null));
    }
    return new AvroRowData(Schema.createRecord(fields));
  }

  @Override
  public RowData createStruct(List<DataType> fieldTypes) {
    return createStruct(IntStream.range(0, fieldTypes.size()).mapToObj(i -> "field" + i).collect(Collectors.toList()),
        fieldTypes);
  }

  @Override
  public RowData createStruct(DataType dataType) {
    return new AvroRowData((Schema) dataType.underlyingType());
  }

  @Override
  public DataType createDataType(String typeSignature) {
    return AvroWrapper.createStdType(_typeFactory.createType(TypeSignature.parse(typeSignature), _boundVariables));
  }
}
