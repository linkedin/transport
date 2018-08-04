package com.linkedin.stdudfs.api.data;

/** A Standard UDF data type for representing strings. */
public interface StdString extends StdData {

  /** Returns the underlying {@link String} value. */
  String get();
}
