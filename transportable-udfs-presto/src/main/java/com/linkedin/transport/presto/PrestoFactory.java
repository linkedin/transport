/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.presto;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.presto.data.PrestoArrayData;
import com.linkedin.transport.presto.data.PrestoMapData;
import com.linkedin.transport.presto.data.PrestoRowData;
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
  public ArrayData createArray(StdType stdType, int expectedSize) {
    return new PrestoArrayData((ArrayType) stdType.underlyingType(), expectedSize, this);
  }

  @Override
  public ArrayData createArray(StdType stdType) {
    return createArray(stdType, 0);
  }

  @Override
  public MapData createMap(StdType stdType) {
    return new PrestoMapData((MapType) stdType.underlyingType(), this);
  }

  @Override
  public PrestoRowData createStruct(List<String> fieldNames, List<StdType> fieldTypes) {
    return new PrestoRowData(fieldNames,
        fieldTypes.stream().map(stdType -> (Type) stdType.underlyingType()).collect(Collectors.toList()), this);
  }

  @Override
  public PrestoRowData createStruct(List<StdType> fieldTypes) {
    return new PrestoRowData(
        fieldTypes.stream().map(stdType -> (Type) stdType.underlyingType()).collect(Collectors.toList()), this);
  }

  @Override
  public RowData createStruct(StdType stdType) {
    return new PrestoRowData((RowType) stdType.underlyingType(), this);
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
