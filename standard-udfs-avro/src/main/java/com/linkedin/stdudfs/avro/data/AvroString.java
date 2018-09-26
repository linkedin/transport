/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.stdudfs.avro.data;

import com.linkedin.stdudfs.api.data.PlatformData;
import com.linkedin.stdudfs.api.data.StdString;
import org.apache.avro.util.Utf8;


public class AvroString implements StdString, PlatformData {
  private Utf8 _string;

  public AvroString(Utf8 string) {
    _string = string;
  }

  @Override
  public String get() {
    return _string.toString();
  }

  @Override
  public Object getUnderlyingData() {
    return _string;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _string = (Utf8) value;
  }
}
