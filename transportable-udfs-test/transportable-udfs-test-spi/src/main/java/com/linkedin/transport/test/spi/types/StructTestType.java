/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi.types;

import java.util.List;
import java.util.Objects;


public class StructTestType implements TestType {

  private final List<String> _fieldNames;
  private final List<TestType> _fieldTypes;

  public StructTestType(List<String> fieldNames, List<TestType> fieldTypes) {
    _fieldNames = fieldNames;
    _fieldTypes = fieldTypes;
  }

  public List<String> getFieldNames() {
    return _fieldNames;
  }

  public List<TestType> getFieldTypes() {
    return _fieldTypes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StructTestType that = (StructTestType) o;
    return Objects.equals(_fieldNames, that._fieldNames) && Objects.equals(_fieldTypes, that._fieldTypes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_fieldNames, _fieldTypes);
  }
}
