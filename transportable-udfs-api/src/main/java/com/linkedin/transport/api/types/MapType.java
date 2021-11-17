/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.types;

/** A {@link DataType} representing a map type. */
public interface MapType extends DataType {

  /** Returns the {@link DataType} of the map keys. */
  DataType keyType();

  /** Returns the {@link DataType} of the map values. */
  DataType valueType();
}
