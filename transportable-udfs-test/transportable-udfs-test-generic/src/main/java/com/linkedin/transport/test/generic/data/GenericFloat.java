/**
 * Copyright 2018 LinkedIn Corporation. All rights reserved.
 * Licensed under the BSD-2 Clause license.
 * See LICENSE in the project root for license information.
 */
package com.linkedin.transport.test.generic.data;

import com.linkedin.transport.api.data.PlatformData;
import com.linkedin.transport.api.data.StdFloat;


public class GenericFloat implements StdFloat, PlatformData {

  private Float _float;

  public GenericFloat(Float aFloat) {
    _float = aFloat;
  }

  @Override
  public float get() {
    return _float;
  }

  @Override
  public Object getUnderlyingData() {
    return _float;
  }

  @Override
  public void setUnderlyingData(Object value) {
    _float = (Float) value;
  }
}
