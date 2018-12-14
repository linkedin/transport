/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi.types;

import java.util.List;


public class TestTypeFactory {

  public static final TestType BOOLEAN_TEST_TYPE = new BooleanTestType();
  public static final TestType INTEGER_TEST_TYPE = new IntegerTestType();
  public static final TestType LONG_TEST_TYPE = new LongTestType();
  public static final TestType STRING_TEST_TYPE = new StringTestType();
  public static final TestType UNKNOWN_TEST_TYPE = new UnknownTestType();

  private TestTypeFactory() {
  }

  public static TestType array(TestType elementType) {
    return new ArrayTestType(elementType);
  }

  public static TestType map(TestType keyType, TestType valueType) {
    return new MapTestType(keyType, valueType);
  }

  public static TestType struct(List<String> fieldNames, List<TestType> fieldTypes) {
    return new StructTestType(fieldNames, fieldTypes);
  }

  public static TestType struct(List<TestType> fieldTypes) {
    return struct(null, fieldTypes);
  }
}
