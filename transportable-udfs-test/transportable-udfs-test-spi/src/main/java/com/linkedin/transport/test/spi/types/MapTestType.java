/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi.types;

import java.util.Objects;


public class MapTestType implements TestType {
  private final TestType _keyType;
  private final TestType _valueType;

  public MapTestType(TestType keyType, TestType valueType) {
    _keyType = keyType;
    _valueType = valueType;
  }

  public TestType getKeyType() {
    return _keyType;
  }

  public TestType getValueType() {
    return _valueType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MapTestType that = (MapTestType) o;
    return Objects.equals(_keyType, that._keyType) && Objects.equals(_valueType, that._valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_keyType, _valueType);
  }
}
