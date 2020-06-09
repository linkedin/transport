/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdBoolean;
import com.linkedin.transport.api.data.StdBytes;
import com.linkedin.transport.api.data.StdDouble;
import com.linkedin.transport.api.data.StdFloat;
import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.data.StdLong;
import com.linkedin.transport.api.data.StdMap;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.data.StdStruct;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.presto.data.PrestoArray;
import com.linkedin.transport.presto.data.PrestoBoolean;
import com.linkedin.transport.presto.data.PrestoBytes;
import com.linkedin.transport.presto.data.PrestoDouble;
import com.linkedin.transport.presto.data.PrestoFloat;
import com.linkedin.transport.presto.data.PrestoInteger;
import com.linkedin.transport.presto.data.PrestoLong;
import com.linkedin.transport.presto.data.PrestoMap;
import com.linkedin.transport.presto.data.PrestoString;
import com.linkedin.transport.presto.data.PrestoStruct;
import io.airlift.slice.Slices;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.OperatorNotFoundException;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.operator.scalar.ScalarFunctionImplementation;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.metadata.SignatureBinder.*;
import static io.prestosql.operator.TypeSignatureParser.*;

public class PrestoFactory implements StdFactory {

  final BoundVariables boundVariables;
  final Metadata metadata;

  public PrestoFactory(BoundVariables boundVariables, Metadata metadata) {
    this.boundVariables = boundVariables;
    this.metadata = metadata;
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
  public StdFloat createFloat(float value) {
    return new PrestoFloat(value);
  }

  @Override
  public StdDouble createDouble(double value) {
    return new PrestoDouble(value);
  }

  @Override
  public StdBytes createBytes(ByteBuffer value) {
    return new PrestoBytes(Slices.wrappedBuffer(value.array()));
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
    return PrestoWrapper.createStdType(
        metadata.getType(applyBoundVariables(parseTypeSignature(typeSignature, ImmutableSet.of()), boundVariables)));
  }

  public ScalarFunctionImplementation getScalarFunctionImplementation(ResolvedFunction resolvedFunction) {
    return metadata.getScalarFunctionImplementation(resolvedFunction);
  }

  public ResolvedFunction resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes) throws OperatorNotFoundException {
    return metadata.resolveOperator(operatorType, argumentTypes);
  }
}
