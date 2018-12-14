/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.spi;

import java.util.List;
import java.util.Objects;


public class Row {

  private final List<Object> _fields;

  public Row(List<Object> fields) {
    _fields = fields;
  }

  public List<Object> getFields() {
    return _fields;
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
    return Objects.equals(_fields, row._fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_fields);
  }
}
