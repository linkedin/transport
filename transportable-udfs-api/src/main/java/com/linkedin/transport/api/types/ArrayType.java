/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.types;

/** A {@link DataType} representing an array type. */
public interface ArrayType extends DataType {

  /** Returns the {@link DataType} of the array elements. */
  DataType elementType();
}
