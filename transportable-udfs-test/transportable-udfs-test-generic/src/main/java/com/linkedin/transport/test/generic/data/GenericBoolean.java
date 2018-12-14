/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdBoolean;


public class GenericBoolean implements StdBoolean, PlatformData {
  private Boolean _boolean;

  public GenericBoolean(Boolean aBoolean) {
    _boolean = aBoolean;
  }

  @Override
  public boolean get() {
    return _boolean;
  }

  @Override
  public Object getUnderlyingData() {
    return _boolean;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _boolean = (Boolean) value;
  }
}
