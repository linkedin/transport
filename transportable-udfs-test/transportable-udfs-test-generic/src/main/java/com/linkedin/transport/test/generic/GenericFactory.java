/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.ArrayData;
import com.linkedin.transport.api.data.MapData;
import com.linkedin.transport.api.data.RowData;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.test.generic.data.GenericArrayData;
import com.linkedin.transport.test.generic.data.GenericMapData;
import com.linkedin.transport.test.generic.data.GenericStruct;
import com.linkedin.transport.test.generic.typesystem.GenericTypeFactory;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.test.spi.types.TestTypeFactory;
import com.linkedin.transport.typesystem.AbstractBoundVariables;
import com.linkedin.transport.typesystem.TypeSignature;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


public class GenericFactory implements StdFactory {

  private final AbstractBoundVariables<TestType> _boundVariables;
  private final GenericTypeFactory _typeFactory;

  public GenericFactory(AbstractBoundVariables<TestType> boundVariables) {
    _boundVariables = boundVariables;
    _typeFactory = new GenericTypeFactory();
  }

  @Override
  public ArrayData createArray(StdType stdType, int expectedSize) {
    return new GenericArrayData(new ArrayList<>(expectedSize), (TestType) stdType.underlyingType());
  }

  @Override
  public ArrayData createArray(StdType stdType) {
    return createArray(stdType, 0);
  }

  @Override
  public MapData createMap(StdType stdType) {
    return new GenericMapData((TestType) stdType.underlyingType());
  }

  @Override
  public RowData createStruct(List<String> fieldNames, List<StdType> fieldTypes) {
    return new GenericStruct(TestTypeFactory.struct(fieldNames,
        fieldTypes.stream().map(x -> (TestType) x.underlyingType()).collect(Collectors.toList())));
  }

  @Override
  public RowData createStruct(List<StdType> fieldTypes) {
    return createStruct(null, fieldTypes);
  }

  @Override
  public RowData createStruct(StdType stdType) {
    return new GenericStruct((TestType) stdType.underlyingType());
  }

  @Override
  public StdType createStdType(String typeSignature) {
    return GenericWrapper.createStdType(_typeFactory.createType(TypeSignature.parse(typeSignature), _boundVariables));
  }
}
