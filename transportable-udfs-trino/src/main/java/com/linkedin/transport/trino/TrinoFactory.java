/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
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
import com.linkedin.transport.trino.data.TrinoArray;
import com.linkedin.transport.trino.data.TrinoBoolean;
import com.linkedin.transport.trino.data.TrinoBinary;
import com.linkedin.transport.trino.data.TrinoDouble;
import com.linkedin.transport.trino.data.TrinoFloat;
import com.linkedin.transport.trino.data.TrinoInteger;
import com.linkedin.transport.trino.data.TrinoLong;
import com.linkedin.transport.trino.data.TrinoMap;
import com.linkedin.transport.trino.data.TrinoString;
import com.linkedin.transport.trino.data.TrinoStruct;
import io.airlift.slice.Slices;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.FunctionDependencies;
import io.trino.metadata.Metadata;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import static com.linkedin.transport.trino.StdUDFUtils.quoteReservedKeywords;
import static io.trino.metadata.SignatureBinder.*;
import static io.trino.sql.analyzer.TypeSignatureTranslator.*;


public class TrinoFactory implements StdFactory {

  final FunctionBinding functionBinding;
  final FunctionDependencies functionDependencies;
  final Metadata metadata;

  public TrinoFactory(FunctionBinding functionBinding, FunctionDependencies functionDependencies) {
    this.functionBinding = functionBinding;
    this.functionDependencies = functionDependencies;
    this.metadata = null;
  }

  public TrinoFactory(FunctionBinding functionBinding, Metadata metadata) {
    this.functionBinding = functionBinding;
    this.functionDependencies = null;
    this.metadata = metadata;
  }

  @Override
  public StdInteger createInteger(int value) {
    return new TrinoInteger(value);
  }

  @Override
  public StdLong createLong(long value) {
    return new TrinoLong(value);
  }

  @Override
  public StdBoolean createBoolean(boolean value) {
    return new TrinoBoolean(value);
  }

  @Override
  public StdString createString(String value) {
    Preconditions.checkNotNull(value, "Cannot create a null StdString");
    return new TrinoString(Slices.utf8Slice(value));
  }

  @Override
  public StdFloat createFloat(float value) {
    return new TrinoFloat(value);
  }

  @Override
  public StdDouble createDouble(double value) {
    return new TrinoDouble(value);
  }

  @Override
  public StdBinary createBinary(ByteBuffer value) {
    return new TrinoBinary(Slices.wrappedBuffer(value.array()));
  }

  @Override
  public StdArray createArray(StdType stdType, int expectedSize) {
    return new TrinoArray((ArrayType) stdType.underlyingType(), expectedSize, this);
  }

  @Override
  public StdArray createArray(StdType stdType) {
    return createArray(stdType, 0);
  }

  @Override
  public StdMap createMap(StdType stdType) {
    return new TrinoMap((MapType) stdType.underlyingType(), this);
  }

  @Override
  public TrinoStruct createStruct(List<String> fieldNames, List<StdType> fieldTypes) {
    return new TrinoStruct(fieldNames,
        fieldTypes.stream().map(stdType -> (Type) stdType.underlyingType()).collect(Collectors.toList()), this);
  }

  @Override
  public TrinoStruct createStruct(List<StdType> fieldTypes) {
    return new TrinoStruct(
        fieldTypes.stream().map(stdType -> (Type) stdType.underlyingType()).collect(Collectors.toList()), this);
  }

  @Override
  public StdStruct createStruct(StdType stdType) {
    return new TrinoStruct((RowType) stdType.underlyingType(), this);
  }

  @Override
  public StdType createStdType(String typeSignature) {
    if (metadata != null) {
      return TrinoWrapper.createStdType(metadata.getType(applyBoundVariables(
          parseTypeSignature(quoteReservedKeywords(typeSignature), ImmutableSet.of()),
          functionBinding)));
    }
    return TrinoWrapper.createStdType(functionDependencies.getType(
        applyBoundVariables(parseTypeSignature(quoteReservedKeywords(typeSignature), ImmutableSet.of()),
            functionBinding)));
  }

  public MethodHandle getOperatorHandle(
      OperatorType operatorType,
      List<Type> argumentTypes,
      InvocationConvention invocationConvention) throws OperatorNotFoundException {
    if (metadata != null) {
      return metadata.getScalarFunctionInvoker(metadata.resolveOperator(operatorType, argumentTypes),
          invocationConvention).getMethodHandle();
    }
    return functionDependencies.getOperatorInvoker(operatorType, argumentTypes, invocationConvention).getMethodHandle();
  }
}
