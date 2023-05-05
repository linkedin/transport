/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.trino;

import com.google.common.collect.ImmutableSet;
import com.linkedin.transport.api.StdFactory;

import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.trino.data.TrinoArrayData;
import com.linkedin.transport.trino.data.TrinoMapData;
import com.linkedin.transport.trino.data.TrinoRowData;
import io.trino.metadata.FunctionBinding;
import io.trino.spi.function.FunctionDependencies;
import io.trino.metadata.OperatorNotFoundException;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.stream.Collectors;

import static com.linkedin.transport.trino.StdUDFUtils.quoteReservedKeywords;
import static io.trino.metadata.SignatureBinder.*;
import static io.trino.sql.analyzer.TypeSignatureTranslator.*;


public class TrinoFactory implements StdFactory {

  final FunctionBinding functionBinding;
  final FunctionDependencies functionDependencies;

  public TrinoFactory(FunctionBinding functionBinding, FunctionDependencies functionDependencies) {
    this.functionBinding = functionBinding;
    this.functionDependencies = functionDependencies;
  }

  @Override
  public ArrayData createArray(StdType stdType, int expectedSize) {
    return new TrinoArrayData((ArrayType) stdType.underlyingType(), expectedSize, this);
  }

  @Override
  public ArrayData createArray(StdType stdType) {
    return createArray(stdType, 0);
  }

  @Override
  public MapData createMap(StdType stdType) {
    return new TrinoMapData((MapType) stdType.underlyingType(), this);
  }

  @Override
  public TrinoRowData createStruct(List<String> fieldNames, List<StdType> fieldTypes) {
    return new TrinoRowData(fieldNames,
        fieldTypes.stream().map(stdType -> (Type) stdType.underlyingType()).collect(Collectors.toList()), this);
  }

  @Override
  public TrinoRowData createStruct(List<StdType> fieldTypes) {
    return new TrinoRowData(
        fieldTypes.stream().map(stdType -> (Type) stdType.underlyingType()).collect(Collectors.toList()), this);
  }

  @Override
  public RowData createStruct(StdType stdType) {
    return new TrinoRowData((RowType) stdType.underlyingType(), this);
  }

  @Override
  public StdType createStdType(String typeSignatureStr) {
    TypeSignature typeSignature = applyBoundVariables(parseTypeSignature(quoteReservedKeywords(typeSignatureStr), ImmutableSet.of()), functionBinding);
    return TrinoWrapper.createStdType(functionDependencies.getType(typeSignature));
  }

  public MethodHandle getOperatorHandle(
      OperatorType operatorType,
      List<Type> argumentTypes,
      InvocationConvention invocationConvention) throws OperatorNotFoundException {
    return functionDependencies.getOperatorImplementation(operatorType, argumentTypes, invocationConvention).getMethodHandle();
  }
}
