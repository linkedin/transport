/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.typesystem;

import com.linkedin.transport.test.spi.types.ArrayTestType;
import com.linkedin.transport.test.spi.types.BooleanTestType;
import com.linkedin.transport.test.spi.types.IntegerTestType;
import com.linkedin.transport.test.spi.types.LongTestType;
import com.linkedin.transport.test.spi.types.MapTestType;
import com.linkedin.transport.test.spi.types.StringTestType;
import com.linkedin.transport.test.spi.types.StructTestType;
import com.linkedin.transport.test.spi.types.TestType;
import com.linkedin.transport.test.spi.types.TestTypeFactory;
import com.linkedin.transport.test.spi.types.UnknownTestType;
import com.linkedin.transport.typesystem.AbstractTypeSystem;
import java.util.List;


public class GenericTypeSystem extends AbstractTypeSystem<TestType> {

  @Override
  protected TestType getArrayElementType(TestType dataType) {
    return ((ArrayTestType) dataType).getElementType();
  }

  @Override
  protected TestType getMapKeyType(TestType dataType) {
    return ((MapTestType) dataType).getKeyType();
  }

  @Override
  protected TestType getMapValueType(TestType dataType) {
    return ((MapTestType) dataType).getValueType();
  }

  @Override
  protected List<TestType> getStructFieldTypes(TestType dataType) {
    return ((StructTestType) dataType).getFieldTypes();
  }

  @Override
  protected boolean isUnknownType(TestType dataType) {
    return dataType instanceof UnknownTestType;
  }

  @Override
  protected boolean isBooleanType(TestType dataType) {
    return dataType instanceof BooleanTestType;
  }

  @Override
  protected boolean isIntegerType(TestType dataType) {
    return dataType instanceof IntegerTestType;
  }

  @Override
  protected boolean isLongType(TestType dataType) {
    return dataType instanceof LongTestType;
  }

  @Override
  protected boolean isStringType(TestType dataType) {
    return dataType instanceof StringTestType;
  }

  @Override
  protected boolean isArrayType(TestType dataType) {
    return dataType instanceof ArrayTestType;
  }

  @Override
  protected boolean isMapType(TestType dataType) {
    return dataType instanceof MapTestType;
  }

  @Override
  protected boolean isStructType(TestType dataType) {
    return dataType instanceof StructTestType;
  }

  @Override
  protected TestType createBooleanType() {
    return TestTypeFactory.BOOLEAN_TEST_TYPE;
  }

  @Override
  protected TestType createIntegerType() {
    return TestTypeFactory.INTEGER_TEST_TYPE;
  }

  @Override
  protected TestType createLongType() {
    return TestTypeFactory.LONG_TEST_TYPE;
  }

  @Override
  protected TestType createStringType() {
    return TestTypeFactory.STRING_TEST_TYPE;
  }

  @Override
  protected TestType createUnknownType() {
    return TestTypeFactory.UNKNOWN_TEST_TYPE;
  }

  @Override
  protected TestType createArrayType(TestType elementType) {
    return TestTypeFactory.array(elementType);
  }

  @Override
  protected TestType createMapType(TestType keyType, TestType valueType) {
    return TestTypeFactory.map(keyType, valueType);
  }

  @Override
  protected TestType createStructType(List<String> fieldNames, List<TestType> fieldTypes) {
    return TestTypeFactory.struct(fieldNames, fieldTypes);
  }
}
