/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.types;

import java.util.List;


/** A {@link DataType} representing a struct type. */
public interface RowType extends DataType {

  /** Returns a {@link List} of the types of all the struct fields. */
  List<? extends DataType> fieldTypes();
}
