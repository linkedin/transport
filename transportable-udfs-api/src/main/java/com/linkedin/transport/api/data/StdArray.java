/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.api.data;

/** A Standard UDF data type for representing arrays. */
public interface StdArray extends StdData, Iterable<StdData> {

  /** Returns the number of elements in the array. */
  int size();

  /**
   * Gets the element at the specified index in the array.
   *
   * @param idx  the index of the element to be retrieved
   */
  StdData get(int idx);

  /**
   * Adds an element to the end of the array.
   *
   * @param e  the element to append to the array
   */
  void add(StdData e);
}
