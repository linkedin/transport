/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.api.types;

/**
 * An interface for all types describing the Standard UDF data types.
 *
 * All Standard UDF types (e.g., {@link StdIntegerType}, {@link StdArrayType}, {@link StdMapType}) are its sub-interfaces.
 * {@link com.linkedin.stdudfs.api.StdFactory#createStdType(String)} can be used to create {@link StdType} objects from
 * a type signature string.
 */
public interface StdType {

  /** Returns the platform-specific schema type for a given {@link StdType}. */
  Object underlyingType();
}
