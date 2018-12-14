/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdLong;


public class GenericLong implements StdLong, PlatformData {
  private Long _long;

  public GenericLong(Long aLong) {
    _long = aLong;
  }

  @Override
  public long get() {
    return _long;
  }

  @Override
  public Object getUnderlyingData() {
    return _long;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _long = (Long) value;
  }
}
