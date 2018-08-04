package com.linkedin.stdudfs.presto.data;

import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdTimestamp;


public class PrestoTimestamp implements StdTimestamp, PlatformData {

  long _epoch;

  @Override
  public long toEpoch() {
    return _epoch;
  }

  @Override
  public Object getUnderlyingData() {
    return _epoch;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _epoch = (long) value;
  }
}
