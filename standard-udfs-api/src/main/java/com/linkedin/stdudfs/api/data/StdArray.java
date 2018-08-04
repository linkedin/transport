package com.linkedin.stdudfs.api.data;

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
