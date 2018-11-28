/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Preconditions;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdBoolean;
import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.data.StdLong;
import com.linkedin.transport.api.data.StdMap;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.data.StdStruct;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.presto.data.PrestoArray;
import com.linkedin.transport.presto.data.PrestoBoolean;
import com.linkedin.transport.presto.data.PrestoInteger;
import com.linkedin.transport.presto.data.PrestoLong;
import com.linkedin.transport.presto.data.PrestoMap;
import com.linkedin.transport.presto.data.PrestoString;
import com.linkedin.transport.presto.data.PrestoStruct;
import com.linkedin.transport.presto.types.PrestoType;
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
