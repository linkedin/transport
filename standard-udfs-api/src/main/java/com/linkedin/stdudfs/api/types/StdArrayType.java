package com.linkedin.stdudfs.api.types;

/** A {@link StdType} representing an array type. */
public interface StdArrayType extends StdType {

  /** Returns the {@link StdType} of the array elements. */
  StdType elementType();
}
