package com.linkedin.stdudfs.presto.data;

import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdBoolean;


public class PrestoBoolean implements StdBoolean, PlatformData {

  boolean _value;

  public PrestoBoolean(boolean value) {
    _value = value;
  }

  @Override
  public boolean get() {
    return _value;
  }

  @Override
  public Object getUnderlyingData() {
    return _value;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _value = (boolean) value;
  }
}
