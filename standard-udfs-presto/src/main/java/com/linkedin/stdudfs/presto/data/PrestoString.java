package com.linkedin.stdudfs.presto.data;

import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdString;
import io.airlift.slice.Slice;


public class PrestoString implements StdString, PlatformData {

  Slice _slice;

  public PrestoString(Slice slice) {
    _slice = slice;
  }

  @Override
  public String get() {
    return _slice.toStringUtf8();
  }

  @Override
  public Object getUnderlyingData() {
    return _slice;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _slice = (Slice) value;
  }
}
