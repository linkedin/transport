package com.linkedin.stdudfs.api.data;

/** An interface for all platform-specific implementations of {@link StdData}. */
public interface PlatformData {

  /** Returns the underlying platform-specific object holding the data. */
  Object getUnderlyingData();

  /** Sets the underlying platform-specific object holding the data. */
  void setUnderlyingData(Object value);
}
