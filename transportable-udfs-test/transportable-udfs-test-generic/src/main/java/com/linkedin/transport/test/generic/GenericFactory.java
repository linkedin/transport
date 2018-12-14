/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic;

import com.linkedin.transport.api.StdFactory;
import com.linkedin.transport.api.data.StdArray;
import com.linkedin.transport.api.data.StdBoolean;
import com.linkedin.transport.api.data.StdInteger;
import com.linkedin.transport.api.data.StdLong;
import com.linkedin.transport.api.data.StdMap;
import com.linkedin.transport.api.data.StdString;
import com.linkedin.transport.api.data.StdStruct;
import com.linkedin.transport.api.types.StdType;
import com.linkedin.transport.test.generic.data.GenericArray;
import com.linkedin.transport.test.generic.data.GenericBoolean;
import com.linkedin.transport.test.generic.data.GenericInteger;
import com.linkedin.transport.test.generic.data.GenericLong;
import com.linkedin.transport.test.generic.data.GenericMap;
import com.linkedin.transport.test.generic.data.GenericString;
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
  public StdInteger createInteger(int value) {
    return new GenericInteger(value);
  }

  @Override
  public StdLong createLong(long value) {
    return new GenericLong(value);
  }

  @Override
  public StdBoolean createBoolean(boolean value) {
    return new GenericBoolean(value);
  }

  @Override
  public StdString createString(String value) {
    return new GenericString(value);
  }

  @Override
  public StdArray createArray(StdType stdType, int expectedSize) {
    return new GenericArray(new ArrayList<>(expectedSize), (TestType) stdType.underlyingType());
  }

  @Override
  public StdArray createArray(StdType stdType) {
    return createArray(stdType, 0);
  }

  @Override
  public StdMap createMap(StdType stdType) {
    return new GenericMap((TestType) stdType.underlyingType());
  }

  @Override
  public StdStruct createStruct(List<String> fieldNames, List<StdType> fieldTypes) {
    return new GenericStruct(TestTypeFactory.struct(fieldNames,
        fieldTypes.stream().map(x -> (TestType) x.underlyingType()).collect(Collectors.toList())));
  }

  @Override
  public StdStruct createStruct(List<StdType> fieldTypes) {
    return createStruct(null, fieldTypes);
  }

  @Override
  public StdStruct createStruct(StdType stdType) {
    return new GenericStruct((TestType) stdType.underlyingType());
  }

  @Override
  public StdType createStdType(String typeSignature) {
    return GenericWrapper.createStdType(_typeFactory.createType(TypeSignature.parse(typeSignature), _boundVariables));
  }
}
