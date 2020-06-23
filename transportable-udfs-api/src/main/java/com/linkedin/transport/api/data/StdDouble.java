/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.data;

/** A Standard UDF data type for representing doubles. */
public interface StdDouble extends StdData {

  /** Returns the underlying double value. */
  double get();
}
