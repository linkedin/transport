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
package com.linkedin.stdudfs.presto;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Preconditions;
import com.linkedin.stdudfs.api.StdFactory;
import com.linkedin.stdudfs.api.data.StdArray;
import com.linkedin.stdudfs.api.data.StdBoolean;
import com.linkedin.stdudfs.api.data.StdInteger;
import com.linkedin.stdudfs.api.data.StdLong;
import com.linkedin.stdudfs.api.data.StdMap;
import com.linkedin.stdudfs.api.data.StdString;
import com.linkedin.stdudfs.api.data.StdStruct;
import com.linkedin.stdudfs.api.types.StdType;
import com.linkedin.stdudfs.presto.data.PrestoArray;
import com.linkedin.stdudfs.presto.data.PrestoBoolean;
import com.linkedin.stdudfs.presto.data.PrestoInteger;
import com.linkedin.stdudfs.presto.data.PrestoLong;
import com.linkedin.stdudfs.presto.data.PrestoMap;
import com.linkedin.stdudfs.presto.data.PrestoString;
import com.linkedin.stdudfs.presto.data.PrestoStruct;
import com.linkedin.stdudfs.presto.types.PrestoType;
import io.airlift.slice.Slices;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.SignatureBinder.*;


public class PrestoFactory implements StdFactory {

  final BoundVariables boundVariables;
  final TypeManager typeManager;
  final FunctionRegistry functionRegistry;

  public PrestoFactory(BoundVariables boundVariables, TypeManager typeManager, FunctionRegistry functionRegistry) {
    this.boundVariables = boundVariables;
    this.typeManager = typeManager;
    this.functionRegistry = functionRegistry;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  @Override
  public StdInteger createInteger(int value) {
    return new PrestoInteger(value);
  }

  @Override
  public StdLong createLong(long value) {
    return new PrestoLong(value);
  }

  @Override
  public StdBoolean createBoolean(boolean value) {
    return new PrestoBoolean(value);
  }

  @Override
  public StdString createString(String value) {
    Preconditions.checkNotNull(value, "Cannot create a null StdString");
    return new PrestoString(Slices.utf8Slice(value));
  }

  @Override
  public StdArray createArray(StdType stdType, int expectedSize) {
    return new PrestoArray((ArrayType) stdType.underlyingType(), expectedSize, this);
  }

  @Override
  public StdArray createArray(StdType stdType) {
    return createArray(stdType, 0);
  }

  @Override
  public StdMap createMap(StdType stdType) {
    return new PrestoMap((MapType) stdType.underlyingType(), this);
  }

  @Override
  public PrestoStruct createStruct(List<String> fieldNames, List<StdType> fieldTypes) {
    return new PrestoStruct(fieldNames,
        fieldTypes.stream().map(stdType -> (Type) stdType.underlyingType()).collect(Collectors.toList()), this);
  }

  @Override
  public PrestoStruct createStruct(List<StdType> fieldTypes) {
    return new PrestoStruct(
        fieldTypes.stream().map(stdType -> (Type) stdType.underlyingType()).collect(Collectors.toList()), this);
  }

  @Override
  public StdStruct createStruct(StdType stdType) {
    return new PrestoStruct((RowType) stdType.underlyingType(), this);
  }

  @Override
  public StdType createStdType(String typeSignature) {
    return new PrestoType(
        typeManager.getType(applyBoundVariables(TypeSignature.parseTypeSignature(typeSignature), boundVariables)));
  }
}
