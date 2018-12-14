/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi.types;

import java.util.Objects;


public class ArrayTestType implements TestType {

  private final TestType _elementType;

  public ArrayTestType(TestType elementType) {
    _elementType = elementType;
  }

  public TestType getElementType() {
    return _elementType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArrayTestType that = (ArrayTestType) o;
    return Objects.equals(_elementType, that._elementType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_elementType);
  }
}
