package com.linkedin.stdudfs.presto.data;

import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdInteger;


public class PrestoInteger implements StdInteger, PlatformData {

  int _integer;

  public PrestoInteger(int integer) {
    _integer = integer;
  }

  @Override
  public int get() {
    return _integer;
  }

  @Override
  public Object getUnderlyingData() {
    return _integer;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _integer = ((Long) value).intValue();
  }
}
