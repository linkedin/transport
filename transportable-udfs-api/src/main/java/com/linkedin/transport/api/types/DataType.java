/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.types;

import com.linkedin.transport.api.TypeFactory;


/**
 * An interface for all types describing the Standard UDF data types.
 *
 * All Standard UDF types (e.g., {@link IntegerType}, {@link ArrayType}, {@link MapType}) are its sub-interfaces.
 * {@link TypeFactory#createDataType(String)} can be used to create {@link DataType} objects from
 * a type signature string.
 */
public interface DataType {

  /** Returns the platform-specific schema type for a given {@link DataType}. */
  Object underlyingType();
}
