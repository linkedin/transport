package com.linkedin.stdudfs.api.data;

/** A Standard UDF data type for representing timestamps. */
public interface StdTimestamp extends StdData {

  /** Returns the number of milliseconds elapsed from epoch for the {@link StdTimestamp}. */
  long toEpoch();
}
