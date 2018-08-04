package com.linkedin.stdudfs.api.types;

import java.util.List;


/** A {@link StdType} representing a struct type. */
public interface StdStructType extends StdType {

  /** Returns a {@link List} of the types of all the struct fields. */
  List<? extends StdType> fieldTypes();
}
