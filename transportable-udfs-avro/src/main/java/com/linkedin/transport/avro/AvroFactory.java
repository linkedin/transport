/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.avro;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdBoolean;
import com.linkedin.transport.api.data.StdBinary;
import com.linkedin.transport.api.data.StdDouble;
import com.linkedin.transport.api.data.StdFloat;
import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.data.StdLong;
import com.linkedin.transport.api.data.StdMap;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.data.StdStruct;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.avro.data.AvroArray;
import com.linkedin.transport.avro.data.AvroBoolean;
import com.linkedin.transport.avro.data.AvroBinary;
import com.linkedin.transport.avro.data.AvroDouble;
import com.linkedin.transport.avro.data.AvroFloat;
import com.linkedin.transport.avro.data.AvroInteger;
import com.linkedin.transport.avro.data.AvroLong;
import com.linkedin.transport.avro.data.AvroMap;
import com.linkedin.transport.avro.data.AvroString;
import com.linkedin.transport.avro.data.AvroStruct;
import com.linkedin.transport.avro.typesystem.AvroTypeFactory;
import com.linkedin.transport.typesystem.AbstractBoundVariables;
import com.linkedin.transport.typesystem.TypeSignature;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;

import static org.apache.avro.Schema.*;


public class AvroFactory implements StdFactory {

  final AbstractBoundVariables<Schema> _boundVariables;
  final AvroTypeFactory _typeFactory;

  public AvroFactory(AbstractBoundVariables<Schema> boundVariables) {
    _boundVariables = boundVariables;
    _typeFactory = new AvroTypeFactory();
  }

  @Override
  public StdInteger createInteger(int value) {
    return new AvroInteger(value);
  }

  @Override
  public StdLong createLong(long value) {
    return new AvroLong(value);
  }

  @Override
  public StdBoolean createBoolean(boolean value) {
    return new AvroBoolean(value);
  }

  @Override
  public StdString createString(String value) {
    return new AvroString(new Utf8(value));
  }

  @Override
  public StdFloat createFloat(float value) {
    return new AvroFloat(value);
  }

  @Override
  public StdDouble createDouble(double value) {
    return new AvroDouble(value);
  }

  @Override
  public StdBinary createBinary(ByteBuffer value) {
    return new AvroBinary(value);
  }

  @Override
  public StdArray createArray(StdType stdType, int size) {
    return new AvroArray((Schema) stdType.underlyingType(), size);
  }

  @Override
  public StdArray createArray(StdType stdType) {
    return createArray(stdType, 0);
  }

  @Override
  public StdMap createMap(StdType stdType) {
    return new AvroMap((Schema) stdType.underlyingType());
  }

  @Override
  public StdStruct createStruct(List<String> fieldNames, List<StdType> fieldTypes) {
    if (fieldNames.size() != fieldTypes.size()) {
      throw new RuntimeException(
          "Field names and types are of different lengths: " + "Field names length is " + fieldNames.size() + ". "
              + "Field types length is: " + fieldTypes.size());
    }
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < fieldTypes.size(); i++) {
      fields.add(new Field(fieldNames.get(i), (Schema) fieldTypes.get(i).underlyingType(), null, null));
    }
    return new AvroStruct(Schema.createRecord(fields));
  }

  @Override
  public StdStruct createStruct(List<StdType> fieldTypes) {
    return createStruct(IntStream.range(0, fieldTypes.size()).mapToObj(i -> "field" + i).collect(Collectors.toList()),
        fieldTypes);
  }

  @Override
  public StdStruct createStruct(StdType stdType) {
    return new AvroStruct((Schema) stdType.underlyingType());
  }

  @Override
  public StdType createStdType(String typeSignature) {
    return AvroWrapper.createStdType(_typeFactory.createType(TypeSignature.parse(typeSignature), _boundVariables));
  }
}
