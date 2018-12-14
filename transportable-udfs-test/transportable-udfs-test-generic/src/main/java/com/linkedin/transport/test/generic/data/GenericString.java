/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdString;


public class GenericString implements StdString, PlatformData {
  private String _string;

  public GenericString(String string) {
    _string = string;
  }

  @Override
  public String get() {
    return _string;
  }

  @Override
  public Object getUnderlyingData() {
    return _string;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _string = (String) value;
  }
}
