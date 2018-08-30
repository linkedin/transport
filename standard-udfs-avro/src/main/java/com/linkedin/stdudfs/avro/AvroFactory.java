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
package com.linkedin.stdudfs.avro;

import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdBoolean;
import com.linkedin.stdudfs.api.data.StdInteger;
import com.linkedin.stdudfs.api.data.StdLong;
import com.linkedin.stdudfs.api.data.StdMap;
import com.linkedin.stdudfs.api.data.StdString;
import com.linkedin.stdudfs.api.data.StdStruct;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.avro.data.AvroArray;
import com.linkedin.stdudfs.avro.data.AvroBoolean;
import com.linkedin.stdudfs.avro.data.AvroInteger;
import com.linkedin.stdudfs.avro.data.AvroLong;
import com.linkedin.stdudfs.avro.data.AvroMap;
import com.linkedin.stdudfs.avro.data.AvroString;
import com.linkedin.stdudfs.avro.data.AvroStruct;
import com.linkedin.stdudfs.avro.typesystem.AvroTypeFactory;
import com.linkedin.stdudfs.typesystem.AbstractBoundVariables;
import com.linkedin.stdudfs.typesystem.TypeSignature;
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
