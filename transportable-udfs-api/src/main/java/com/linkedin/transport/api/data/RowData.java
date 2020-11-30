/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.data;

import java.util.List;


/** A Standard UDF data type for representing SQL ROW/STRUCT data type. */
public interface RowData {

  /**
   * Returns the value of the field at the given position in the row.
   *
   * @param index  the position of the field in the row
   */
  Object getField(int index);

  /**
   * Returns the value of the given field from the row.
   *
   * @param name  the name of the field
   */
  Object getField(String name);

  /**
   * Sets the value of the field at the given position in the row.
   *
   * @param index  the position of the field in the row
   * @param value  the value to be set
   */
  void setField(int index, Object value);

  /**
   * Sets the value of the given field in the row.
   *
   * @param name  the name of the field
   * @param value  the value to be set
   */
  void setField(String name, Object value);

  /** Returns a {@link List} of all fields in the struct. */
  List<Object> fields();
}
