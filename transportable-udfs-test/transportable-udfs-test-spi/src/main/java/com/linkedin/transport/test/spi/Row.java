/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;


public class Row {

  private final List<Object> _fields;
  private List<String> _fieldNames;

  public Row(List<Object> fields) {
    _fields = fields;
  }

  public Row fieldNames(String... fieldNames) {
    this._fieldNames = Arrays.asList(fieldNames);
    return this;
  }

  public List<Object> getFields() {
    return _fields;
  }

  public List<String> getFieldNames() {
    return this._fieldNames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Row row = (Row) o;
    boolean isEquals = Objects.equals(_fields, row._fields);
    if (this._fieldNames != null && row.getFieldNames() != null) {
      // This is important. Only match fieldNames if both of the entities have provided it.
      // We do this to allow interoperability with older code
      isEquals = isEquals && Objects.equals(this._fieldNames, row.getFieldNames());
    }
    return isEquals;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_fields);
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("{");
    stringBuilder.append("fieldNames: ");
    if (this._fieldNames != null) {
      stringBuilder.append(_fieldNames.toString());
    }
    stringBuilder.append(", ");
    stringBuilder.append("fieldValues: ");
    stringBuilder.append(_fields.toString());
    return stringBuilder.toString();
  }
}
