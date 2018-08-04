package com.linkedin.stdudfs.presto.data;

import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdLong;


public class PrestoLong implements StdLong, PlatformData {

  long _value;

  public PrestoLong(long value) {
    _value = value;
  }

  @Override
  public long get() {
    return _value;
  }

  @Override
  public Object getUnderlyingData() {
    return _value;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _value = (long) value;
  }
}
